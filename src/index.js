import express from "express";
import axios from "axios";
import fs from "fs";
import os from "os";
import path from "path";
import { spawn } from "child_process";
import { v4 as uuidv4 } from "uuid";
import { v2 as cloudinary } from "cloudinary";
import pLimit from "p-limit";

// ================= CONFIG =================
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY;

const MAX_CLIPS = 20;
const MAX_CONCURRENT_JOBS = 2;
const DOWNLOAD_CONCURRENCY = 3;
const FFMPEG_TIMEOUT = 240000;

const OUTPUT_WIDTH = 480;
const OUTPUT_HEIGHT = 854;

const USE_GPU = process.env.USE_GPU === "true";

if (!API_KEY) {
  console.error("❌ Missing API_KEY");
  process.exit(1);
}

// ================= CLOUDINARY =================
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

// ================= APP =================
const app = express();
app.use(express.json({ limit: "10mb" }));

// ================= JOB STORE =================
const jobs = new Map();

// ================= RATE LIMIT =================
const requestCounts = new Map();
setInterval(() => requestCounts.clear(), 60000);

app.use((req, res, next) => {
  const ip = req.ip;
  const count = requestCounts.get(ip) || 0;

  if (count > 30) {
    return res.status(429).json({ error: "Rate limit exceeded" });
  }

  requestCounts.set(ip, count + 1);
  next();
});

// ================= AUTH =================
const auth = (req, res, next) => {
  if (req.headers["x-api-key"] !== API_KEY) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
};

// ================= QUEUE =================
const jobLimit = pLimit(MAX_CONCURRENT_JOBS);

// ================= LOGGER =================
const log = (msg, data = {}) => {
  console.log(JSON.stringify({
    msg,
    ...data,
    time: new Date().toISOString()
  }));
};

// ================= WEBHOOK SAFE SENDER =================
const sendWebhook = async (url, payload) => {
  if (!url) return;
  try {
    await axios.post(url, payload, { timeout: 10000 });
  } catch (err) {
    log("webhook_failed", { error: err.message });
  }
};

// ================= HELPERS =================
const updateJob = (id, patch) => {
  const job = jobs.get(id);
  if (!job) return;
  const updated = { ...job, ...patch };
  jobs.set(id, updated);
  return updated;
};

const extractSceneNumber = (str) => {
  const match = str.match(/(\d+)/g);
  return match ? parseInt(match.pop(), 10) : Number.MAX_SAFE_INTEGER;
};

const normalizeUrl = (url) => {
  if (url.includes("player.cloudinary.com")) {
    const match = url.match(/public_id=([^&]+)/);
    if (!match) return url;

    return `https://res.cloudinary.com/${process.env.CLOUDINARY_CLOUD_NAME}/video/upload/${match[1]}.mp4`;
  }
  return url;
};

// ================= DOWNLOAD =================
const downloadFile = async (url, outputPath) => {
  const cleanUrl = normalizeUrl(url);

  const res = await axios({
    method: "GET",
    url: cleanUrl,
    responseType: "stream",
    timeout: 60000,
    validateStatus: s => s < 400
  });

  await new Promise((resolve, reject) => {
    const writer = fs.createWriteStream(outputPath);
    res.data.pipe(writer);
    writer.on("finish", resolve);
    writer.on("error", reject);
  });

  if (!fs.existsSync(outputPath) || fs.statSync(outputPath).size === 0) {
    throw new Error("Downloaded file invalid or empty");
  }
};

// ================= FFMPEG =================
const runFFmpeg = (args, jobId, webhook) => {
  return new Promise((resolve, reject) => {
    const ffmpeg = spawn("ffmpeg", args);

    const timeout = setTimeout(() => {
      ffmpeg.kill("SIGKILL");
      reject(new Error("FFmpeg timeout"));
    }, FFMPEG_TIMEOUT);

    ffmpeg.stderr.on("data", async (d) => {
      const output = d.toString();
      log("ffmpeg", { output });

      const timeMatch = output.match(/time=(\d+:\d+:\d+\.\d+)/);
      if (timeMatch) {
        const progress = timeMatch[1];

        updateJob(jobId, { progress });

        await sendWebhook(webhook, {
          jobId,
          status: "processing",
          progress
        });
      }
    });

    ffmpeg.on("close", code => {
      clearTimeout(timeout);
      if (code !== 0) {
        log("ffmpeg_failed", { code });
        return reject(new Error(`FFmpeg exit ${code}`));
      }
      resolve();
    });
  });
};

// ================= NORMALIZE =================
const normalizeClip = async (input, output, jobId, webhook) => {
  await runFFmpeg([
    "-y",
    "-i", input,

    // ✅ ADD SILENT AUDIO TRACK (CRITICAL FIX)
    "-f", "lavfi",
    "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",

    "-shortest",

    // ✅ SAFE SCALING (FIXES DIMENSION MISMATCH)
    "-vf", `scale=${OUTPUT_WIDTH}:${OUTPUT_HEIGHT}:force_original_aspect_ratio=decrease,pad=${OUTPUT_WIDTH}:${OUTPUT_HEIGHT}:(ow-iw)/2:(oh-ih)/2,fps=30`,

    "-c:v", USE_GPU ? "h264_nvenc" : "libx264",
    "-preset", "veryfast",
    "-crf", "23",

    "-c:a", "aac",
    "-ar", "44100",
    "-ac", "2",

    "-movflags", "+faststart",
    output
  ], jobId, webhook);
};

// ================= ROUTES =================
app.get("/", (_, res) => res.send("FFmpeg service running 🚀"));
app.get("/health", (_, res) => res.json({ ok: true }));

app.get("/status/:id", auth, (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: "Not found" });
  res.json(job);
});

app.get("/jobs", auth, (_, res) => {
  res.json([...jobs.values()]);
});

// ================= MERGE =================
app.post("/merge", auth, async (req, res) => {
  const requestId = uuidv4();

  jobs.set(requestId, {
    id: requestId,
    status: "queued",
    progress: 0
  });

  jobLimit(async () => {
    let tempDir;

    let { clips, webhook } = req.body;

    try {
      await sendWebhook(webhook, {
        jobId: requestId,
        status: "started"
      });

      updateJob(requestId, { status: "processing" });

      if (!clips || clips.length < 2) throw new Error("Need at least 2 clips");
      if (clips.length > MAX_CLIPS) throw new Error("Too many clips");

      clips = clips.sort(
        (a, b) => extractSceneNumber(a) - extractSceneNumber(b)
      );

      tempDir = path.join(os.tmpdir(), requestId);
      fs.mkdirSync(tempDir, { recursive: true });

      // ================= DOWNLOAD =================
      const limit = pLimit(DOWNLOAD_CONCURRENCY);

      const localClips = await Promise.all(
        clips.map((url, i) =>
          limit(async () => {
            const file = path.join(tempDir, `clip_${i}.mp4`);
            await downloadFile(url, file);
            return file;
          })
        )
      );

      updateJob(requestId, { step: "normalizing" });

      // ================= NORMALIZE =================
      const normalized = [];

      for (let i = 0; i < localClips.length; i++) {
        const out = path.join(tempDir, `norm_${i}.mp4`);
        await normalizeClip(localClips[i], out, requestId, webhook);
        normalized.push(out);
      }

      updateJob(requestId, { step: "merging" });

      // ================= CONCAT =================
      const filter =
        normalized.map((_, i) => `[${i}:v:0][${i}:a:0]`).join("") +
        `concat=n=${normalized.length}:v=1:a=1[outv][outa]`;

      const outputPath = path.join(tempDir, "output.mp4");

      await runFFmpeg([
        "-y",
        ...normalized.flatMap(c => ["-i", c]),
        "-filter_complex", filter,
        "-map", "[outv]",
        "-map", "[outa]",
        "-c:v", USE_GPU ? "h264_nvenc" : "libx264",
        "-preset", "veryfast",
        "-crf", "23",
        "-c:a", "aac",
        "-movflags", "+faststart",
        outputPath
      ], requestId, webhook);

      updateJob(requestId, { step: "uploading" });

      log("upload_start", {
        path: outputPath,
        size: fs.statSync(outputPath).size
      });

      // ================= CLOUDINARY STREAM UPLOAD =================
      const upload = await new Promise((resolve, reject) => {
        const stream = cloudinary.uploader.upload_stream(
          {
            resource_type: "video",
            folder: "ai-movies",
          },
          (err, result) => {
            if (err) return reject(err);
            resolve(result);
          }
        );

        fs.createReadStream(outputPath).pipe(stream);
      });

      fs.rmSync(tempDir, { recursive: true, force: true });

      const result = {
        jobId: requestId,
        status: "done",
        url: upload.secure_url,
        duration: upload.duration
      };

      jobs.set(requestId, { ...jobs.get(requestId), ...result });

      await sendWebhook(webhook, result);

    } catch (err) {
      const failPayload = {
        jobId: requestId,
        status: "failed",
        error: err?.response?.data || err.message
      };

      jobs.set(requestId, {
        ...jobs.get(requestId),
        ...failPayload
      });

      await sendWebhook(webhook, failPayload);

      if (tempDir) fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  res.json({
    jobId: requestId,
    statusUrl: `/status/${requestId}`
  });
});

// ================= START =================
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
