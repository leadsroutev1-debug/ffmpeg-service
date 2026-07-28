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

// ⏱ Prevent Render timeout killing response
app.use((req, res, next) => {
  res.setTimeout(300000);
  next();
});

// 🔐 Auth middleware
const auth = (req, res, next) => {
  if (!API_KEY || req.headers["x-api-key"] !== API_KEY) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
};

// ❤️ Health check
app.get("/", (req, res) => {
  res.send("FFmpeg service running 🚀");
});

// 🧠 Extract scene number
const extractSceneNumber = (str) => {
  const match = str.match(/(\d+)/g);
  if (!match) return Number.MAX_SAFE_INTEGER;
  return parseInt(match[match.length - 1], 10);
};

// 📥 Download file
const downloadFile = async (url, outputPath) => {
  console.log("⬇️ Downloading:", url);

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

    if (clips.length > 20) {
      return res.status(400).json({ error: "Too many clips" });
    }

    // 🧠 Sort clips
    clips = clips.sort(
      (a, b) => extractSceneNumber(a) - extractSceneNumber(b)
    );

    console.log("🎞 Sorted clips:", clips);

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

    // 🎥 FIXED FFmpeg pipeline
    await new Promise((resolve, reject) => {
      const filter = `
${localClips
  .map((_, i) => `[${i}:v]scale=480:-2[v${i}]`)
  .join(";")}
${localClips.map((_, i) => `[v${i}]`).join("")}
concat=n=${localClips.length}:v=1:a=0[outv]
`;

      const ffmpegArgs = [
        "-y",

        // inputs
        ...localClips.flatMap((clip) => ["-i", clip]),

        "-filter_complex", filter,

        "-map", "[outv]",

        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-crf", "28",
        "-r", "24",

        outputPath,
      ];

      console.log("🚀 Running FFmpeg...");
      const ffmpeg = spawn("ffmpeg", ffmpegArgs);

      ffmpeg.stderr.on("data", (data) => {
        console.log(data.toString());
      });

      ffmpeg.on("close", (code) => {
        if (code !== 0) {
          return reject(new Error(`FFmpeg failed with code ${code}`));
        }
        resolve();
      });
    });

    console.log("☁️ Uploading...");

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

// ✅ FFmpeg check
spawn("ffmpeg", ["-version"]).on("close", (code) => {
  if (code === 0) console.log("✅ FFmpeg ready");
  else console.error("❌ FFmpeg missing");
});

// 🚀 Start server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
