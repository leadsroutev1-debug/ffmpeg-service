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

// 🔐 Cloudinary
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

// ⏱ Prevent timeout (5 min)
app.use((req, res, next) => {
  res.setTimeout(300000);
  next();
});

// 🔐 Auth
const auth = (req, res, next) => {
  if (!API_KEY || req.headers["x-api-key"] !== API_KEY) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
};

// ❤️ Health
app.get("/", (req, res) => {
  res.send("FFmpeg service running 🚀");
});

// 🧠 Sort scenes
const extractSceneNumber = (str) => {
  const match = str.match(/(\d+)/g);
  return match ? parseInt(match.pop(), 10) : Number.MAX_SAFE_INTEGER;
};

// 🔗 Normalize Cloudinary URLs
const normalizeUrl = (url) => {
  if (url.includes("player.cloudinary.com")) {
    const match = url.match(/public_id=([^&]+)/);
    if (!match) return url;

    return `https://res.cloudinary.com/${process.env.CLOUDINARY_CLOUD_NAME}/video/upload/${match[1]}.mp4`;
  }
  return url;
};

// 📥 Download file
const downloadFile = async (url, outputPath) => {
  const cleanUrl = normalizeUrl(url);
  console.log("⬇️ Downloading:", cleanUrl);

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
};

// 🎬 MERGE
app.post("/merge", auth, async (req, res) => {
  let tempDir;

  try {
    let { clips } = req.body;

    if (!clips || clips.length < 2) {
      return res.status(400).json({ error: "Need at least 2 clips" });
    }

    if (clips.length > 20) {
      return res.status(400).json({ error: "Too many clips (max 20)" });
    }

    // 🧠 Sort clips
    clips = clips.sort(
      (a, b) => extractSceneNumber(a) - extractSceneNumber(b)
    );

    console.log("🎞 Sorted clips:", clips);

    // 📁 Temp dir
    const jobId = uuidv4();
    tempDir = path.join(process.cwd(), "tmp", jobId);
    fs.mkdirSync(tempDir, { recursive: true });

    // 📥 Download all clips
    const localClips = await Promise.all(
      clips.map(async (url, i) => {
        const filePath = path.join(tempDir, `clip_${i}.mp4`);
        await downloadFile(url, filePath);
        return filePath;
      })
    );

    const outputPath = path.join(tempDir, "output.mp4");

    // 🎥 Build filter graph
    const filterParts = [];

    localClips.forEach((_, i) => {
      // 🎥 Normalize video
      filterParts.push(
        `[${i}:v]scale=480:854:force_original_aspect_ratio=decrease,` +
        `pad=480:854:(ow-iw)/2:(oh-ih)/2,` +
        `fps=24,format=yuv420p,setsar=1[v${i}]`
      );

      // 🔊 Handle audio safely:
      // If missing → generate silent audio
      filterParts.push(
        `[${i}:a]aformat=sample_rates=44100:channel_layouts=stereo,aresample=async=1[a${i}]`
      );
    });

    const videoInputs = localClips.map((_, i) => `[v${i}]`).join("");
    const audioInputs = localClips.map((_, i) => `[a${i}]`).join("");

    // 🎬 Concat with audio
    filterParts.push(
      `${videoInputs}${audioInputs}concat=n=${localClips.length}:v=1:a=1[outv][outa]`
    );

    const filter = filterParts.join(";");

    console.log("🧠 FILTER:", filter);

    const ffmpegArgs = [
      "-y",

      // inputs
      ...localClips.flatMap((clip) => ["-i", clip]),

      "-filter_complex", filter,

      "-map", "[outv]",
      "-map", "[outa]",

      // 🎥 video encoding
      "-c:v", "libx264",
      "-preset", "veryfast",
      "-crf", "23",

      // 🔊 audio encoding
      "-c:a", "aac",
      "-b:a", "128k",

      // ⚡ streaming optimization
      "-movflags", "+faststart",

      outputPath,
    ];

    console.log("🚀 FFmpeg args:", ffmpegArgs.join(" "));

    await new Promise((resolve, reject) => {
      const ffmpeg = spawn("ffmpeg", ffmpegArgs);

      ffmpeg.stderr.on("data", (data) => {
        console.log(data.toString());
      });

      ffmpeg.on("error", reject);

      ffmpeg.on("close", (code) => {
        if (code !== 0) {
          return reject(new Error(`FFmpeg failed with code ${code}`));
        }
        resolve();
      });
    });

    console.log("☁️ Uploading to Cloudinary...");

    const uploadResult = await cloudinary.uploader.upload(outputPath, {
      resource_type: "video",
      folder: "ai-movies",
    });

    // 🧹 Cleanup
    fs.rmSync(tempDir, { recursive: true, force: true });

    return res.json({
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

    return res.status(500).json({
      error: "Processing failed",
      details: err.message,
    });
  }
});

// ✅ FFmpeg check
spawn("ffmpeg", ["-version"]).on("close", (code) => {
  if (code === 0) console.log("✅ FFmpeg ready");
  else console.error("❌ FFmpeg missing");
});

// 🚀 Start server
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
