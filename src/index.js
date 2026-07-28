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
const MAX_CONCURRENT_JOBS = 2;     // 🔥 tune for your CPU
const DOWNLOAD_CONCURRENCY = 3;    // 🔥 tune for bandwidth
const FFMPEG_TIMEOUT = 240000;     // 4 min safety kill

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

// prevent timeout
app.use((req, res, next) => {
  res.setTimeout(300000);
  next();
});

// ================= SIMPLE JOB QUEUE =================
const jobLimit = pLimit(MAX_CONCURRENT_JOBS);

// ================= AUTH =================
const auth = (req, res, next) => {
  if (req.headers["x-api-key"] !== API_KEY) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
};

// ================= HEALTH =================
app.get("/", (_, res) => res.send("FFmpeg service running 🚀"));
app.get("/health", (_, res) => res.json({ ok: true }));
app.get("/ready", (_, res) => res.json({ ready: true }));

// ================= HELPERS =================
const log = (msg, data = {}) => {
  console.log(JSON.stringify({ msg, ...data, time: new Date().toISOString() }));
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

// ================= DOWNLOAD (RETRY + STREAM) =================
const downloadFile = async (url, outputPath, retries = 3) => {
  const cleanUrl = normalizeUrl(url);

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      log("⬇️ Downloading", { url: cleanUrl, attempt });

      const response = await axios({
        method: "GET",
        url: cleanUrl,
        responseType: "stream",
        timeout: 60000,
      });

      await new Promise((resolve, reject) => {
        const writer = fs.createWriteStream(outputPath);
        response.data.pipe(writer);
        writer.on("finish", resolve);
        writer.on("error", reject);
      });

      return;
    } catch (err) {
      if (attempt === retries) throw err;
      await new Promise((r) => setTimeout(r, 1000 * attempt));
    }
  }
};

// ================= FFMPEG RUN =================
const runFFmpeg = (args) => {
  return new Promise((resolve, reject) => {
    const ffmpeg = spawn("ffmpeg", args);

    const timeout = setTimeout(() => {
      ffmpeg.kill("SIGKILL");
      reject(new Error("FFmpeg timeout"));
    }, FFMPEG_TIMEOUT);

    ffmpeg.stderr.on("data", (data) => {
      log("ffmpeg", { output: data.toString() });
    });

    ffmpeg.on("error", reject);

    ffmpeg.on("close", (code) => {
      clearTimeout(timeout);
      if (code !== 0) return reject(new Error(`FFmpeg exited ${code}`));
      resolve();
    });
  });
};

// ================= MERGE =================
app.post("/merge", auth, async (req, res) => {
  return jobLimit(async () => {
    let tempDir;

    try {
      let { clips } = req.body;

      if (!clips || clips.length < 2) {
        return res.status(400).json({ error: "Need at least 2 clips" });
      }

      if (clips.length > MAX_CLIPS) {
        return res.status(400).json({ error: `Max ${MAX_CLIPS} clips` });
      }

      clips = clips.sort(
        (a, b) => extractSceneNumber(a) - extractSceneNumber(b)
      );

      const jobId = uuidv4();
      tempDir = path.join(os.tmpdir(), jobId);
      fs.mkdirSync(tempDir, { recursive: true });

      log("🎬 Job start", { jobId, clips: clips.length });

      // 🔥 parallel download with limit
      const limit = pLimit(DOWNLOAD_CONCURRENCY);

      const localClips = await Promise.all(
        clips.map((url, i) =>
          limit(async () => {
            const filePath = path.join(tempDir, `clip_${i}.mp4`);
            await downloadFile(url, filePath);
            return filePath;
          })
        )
      );

      const outputPath = path.join(tempDir, "output.mp4");

      // ================= FILTER =================
      const filterParts = [];

      localClips.forEach((_, i) => {
        filterParts.push(
          `[${i}:v]scale=480:854:force_original_aspect_ratio=decrease,` +
          `pad=480:854:(ow-iw)/2:(oh-ih)/2,fps=24,format=yuv420p,setsar=1[v${i}]`
        );

        filterParts.push(
          `[${i}:a]aformat=sample_rates=44100:channel_layouts=stereo,aresample=async=1[a${i}]`
        );
      });

      const v = localClips.map((_, i) => `[v${i}]`).join("");
      const a = localClips.map((_, i) => `[a${i}]`).join("");

      filterParts.push(
        `${v}${a}concat=n=${localClips.length}:v=1:a=1[outv][outa]`
      );

      const ffmpegArgs = [
        "-y",
        ...localClips.flatMap((c) => ["-i", c]),
        "-filter_complex", filterParts.join(";"),
        "-map", "[outv]",
        "-map", "[outa]",
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "23",
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart",
        outputPath,
      ];

      await runFFmpeg(ffmpegArgs);

      log("☁️ Uploading", { jobId });

      const upload = await cloudinary.uploader.upload(outputPath, {
        resource_type: "video",
        folder: "ai-movies",
        chunk_size: 6000000, // 🔥 large uploads safer
      });

      fs.rmSync(tempDir, { recursive: true, force: true });

      log("✅ Job complete", { jobId });

      return res.json({
        success: true,
        url: upload.secure_url,
        duration: upload.duration,
        clipsProcessed: clips.length,
      });

    } catch (err) {
      log("❌ Job failed", { error: err.message });

      if (tempDir) {
        fs.rmSync(tempDir, { recursive: true, force: true });
      }

      return res.status(500).json({
        error: "Processing failed",
        details: err.message,
      });
    }
  });
});

// ================= FFMPEG CHECK =================
spawn("ffmpeg", ["-version"]).on("close", (code) => {
  if (code === 0) console.log("✅ FFmpeg ready");
  else console.error("❌ FFmpeg missing");
});

// ================= GRACEFUL SHUTDOWN =================
process.on("SIGTERM", () => {
  console.log("🛑 Shutting down...");
  process.exit(0);
});

// ================= START =================
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
