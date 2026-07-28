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

const MAX_FILE_SIZE_MB = 100;
const OUTPUT_WIDTH = 480;
const OUTPUT_HEIGHT = 854;

// 🔥 GPU toggle
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

// ================= JOB STORE (🔥 SWAP WITH REDIS LATER) =================
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

// ================= HELPERS =================
const updateJob = (id, patch) => {
  const job = jobs.get(id);
  if (!job) return;
  jobs.set(id, { ...job, ...patch });
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
  });

  await new Promise((resolve, reject) => {
    const writer = fs.createWriteStream(outputPath);
    res.data.pipe(writer);
    writer.on("finish", resolve);
    writer.on("error", reject);
  });
};

// ================= FFMPEG =================
const runFFmpeg = (args, jobId) => {
  return new Promise((resolve, reject) => {
    const ffmpeg = spawn("ffmpeg", args);

    const timeout = setTimeout(() => {
      ffmpeg.kill("SIGKILL");
      reject(new Error("FFmpeg timeout"));
    }, FFMPEG_TIMEOUT);

    ffmpeg.stderr.on("data", d => {
      const output = d.toString();
      log("ffmpeg", { output });

      // 🔥 basic progress extraction
      const timeMatch = output.match(/time=(\d+:\d+:\d+\.\d+)/);
      if (timeMatch) {
        updateJob(jobId, { progress: timeMatch[1] });
      }
    });

    ffmpeg.on("close", code => {
      clearTimeout(timeout);
      if (code !== 0) return reject(new Error(`FFmpeg exit ${code}`));
      resolve();
    });
  });
};

// ================= HEALTH =================
app.get("/", (_, res) => res.send("FFmpeg service running 🚀"));
app.get("/health", (_, res) => res.json({ ok: true }));

// ================= STATUS =================
app.get("/status/:id", auth, (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: "Not found" });
  res.json(job);
});

// ================= DASHBOARD =================
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

    try {
      updateJob(requestId, { status: "processing" });

      let { clips, webhook, transition = "none" } = req.body;

      if (!clips || clips.length < 2) throw new Error("Need at least 2 clips");

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

      updateJob(requestId, { step: "encoding" });

      // ================= FILTER =================
      let filter = "";

      if (transition === "fade") {
        // 🔥 crossfade example
        filter = `
        [0:v][1:v]xfade=transition=fade:duration=1:offset=4[v];
        [0:a][1:a]acrossfade=d=1[a]
        `;
      } else {
        const v = localClips.map((_, i) => `[${i}:v]`).join("");
        const a = localClips.map((_, i) => `[${i}:a?]`).join("");

        filter = `${v}${a}concat=n=${localClips.length}:v=1:a=1[outv][outa]`;
      }

      const outputPath = path.join(tempDir, "output.mp4");

      await runFFmpeg([
        "-y",
        ...localClips.flatMap(c => ["-i", c]),
        "-filter_complex", filter,
        "-map", "[outv]",
        "-map", "[outa]",
        "-c:v", USE_GPU ? "h264_nvenc" : "libx264",
        "-preset", "veryfast",
        "-crf", "23",
        "-c:a", "aac",
        "-movflags", "+faststart",
        outputPath
      ], requestId);

      updateJob(requestId, { step: "uploading" });

      const upload = await cloudinary.uploader.upload(outputPath, {
        resource_type: "video",
        folder: "ai-movies",
      });

      fs.rmSync(tempDir, { recursive: true, force: true });

      const result = {
        status: "done",
        url: upload.secure_url,
        duration: upload.duration
      };

      jobs.set(requestId, { ...jobs.get(requestId), ...result });

      if (webhook) {
        axios.post(webhook, result).catch(() => {});
      }

    } catch (err) {
      jobs.set(requestId, {
        ...jobs.get(requestId),
        status: "failed",
        error: err.message
      });
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
