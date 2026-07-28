import express from "express";
import axios from "axios";
import fs from "fs";
import path from "path";
import { spawn } from "child_process";
import { v4 as uuidv4 } from "uuid";
import { v2 as cloudinary } from "cloudinary";

const app = express();
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY;

// 🔐 Cloudinary config
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

// ⏱ Prevent Render timeout killing early
app.use((req, res, next) => {
  res.setTimeout(120000); // 2 minutes
  next();
});

// 🔐 Scoped auth middleware
const auth = (req, res, next) => {
  if (!API_KEY || req.headers["x-api-key"] !== API_KEY) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
};

// ❤️ Health route (NO AUTH)
app.get("/", (req, res) => {
  res.send("Smart FFmpeg + Cloudinary service 🚀");
});

// 🧠 Extract number for sorting
const extractSceneNumber = (str) => {
  const match = str.match(/(\d+)/g);
  if (!match) return Number.MAX_SAFE_INTEGER;
  return parseInt(match[match.length - 1], 10);
};

// 📥 Download helper (safer)
const downloadFile = async (url, outputPath) => {
  const response = await axios({
    method: "GET",
    url,
    responseType: "stream",
    timeout: 30000,
  });

  await new Promise((resolve, reject) => {
    const writer = fs.createWriteStream(outputPath);
    response.data.pipe(writer);
    writer.on("finish", resolve);
    writer.on("error", reject);
  });
};

// 🎬 MERGE ENDPOINT
app.post("/merge", auth, async (req, res) => {
  let tempDir;

  try {
    let { clips } = req.body;

    if (!clips || clips.length < 2) {
      return res.status(400).json({ error: "Need at least 2 clips" });
    }

    if (clips.length > 50) {
      return res.status(400).json({ error: "Too many clips" });
    }

    // 🧠 Sort clips
    clips = clips.sort(
      (a, b) => extractSceneNumber(a) - extractSceneNumber(b)
    );

    console.log("Sorted clips:", clips);

    const jobId = uuidv4();
    tempDir = `/tmp/${jobId}`;
    fs.mkdirSync(tempDir, { recursive: true });

    // ⚡ Download clips
    const localClips = await Promise.all(
      clips.map(async (url, i) => {
        const filePath = path.join(tempDir, `clip_${i}.mp4`);
        await downloadFile(url, filePath);
        return filePath;
      })
    );

    // 🧾 Create concat file
    const concatPath = path.join(tempDir, "concat.txt");
    fs.writeFileSync(
      concatPath,
      localClips.map((c) => `file '${c}'`).join("\n")
    );

    const outputPath = path.join(tempDir, "output.mp4");

    // 🎥 FFmpeg merge (ROBUST)
    await new Promise((resolve, reject) => {
      const ffmpeg = spawn("ffmpeg", [
        "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", concatPath,

        // 🔥 Normalize everything (THIS FIXES MOST FAILURES)
        "-vf",
        "scale=720:1280:force_original_aspect_ratio=decrease,pad=720:1280:(ow-iw)/2:(oh-ih)/2",
        "-r", "30",

        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "23",

        "-c:a", "aac",
        "-b:a", "128k",

        outputPath
      ]);

      ffmpeg.stderr.on("data", (data) => {
        console.log("FFmpeg:", data.toString());
      });

      ffmpeg.on("close", (code) => {
        if (code !== 0) {
          return reject(new Error(`FFmpeg failed with code ${code}`));
        }
        resolve();
      });
    });

    // ☁️ Upload to Cloudinary
    const uploadResult = await cloudinary.uploader.upload(outputPath, {
      resource_type: "video",
      folder: "ai-movies",
    });

    // 🧹 Cleanup
    fs.rmSync(tempDir, { recursive: true, force: true });

    res.json({
      success: true,
      url: uploadResult.secure_url,
      duration: uploadResult.duration,
      clipsProcessed: clips.length,
    });

  } catch (err) {
    console.error("❌ ERROR:", err);

    if (tempDir) {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }

    res.status(500).json({
      error: "Processing failed",
      details: err.message,
    });
  }
});

// ✅ FFmpeg check (startup)
const ffmpegCheck = spawn("ffmpeg", ["-version"]);
ffmpegCheck.on("close", (code) => {
  if (code === 0) console.log("✅ FFmpeg ready");
  else console.error("❌ FFmpeg missing");
});

// 🚀 Start server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
