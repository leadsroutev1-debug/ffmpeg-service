import express from "express";
import axios from "axios";
import fs from "fs";
import { exec } from "child_process";
import { v4 as uuidv4 } from "uuid";
import { v2 as cloudinary } from "cloudinary";

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY;

// 🔐 Cloudinary config
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
});

// 🔐 Auth middleware
app.use((req, res, next) => {
  if (req.headers["x-api-key"] !== API_KEY) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
});

// ❤️ Health check
app.get("/", (req, res) => {
  res.send("FFmpeg + Cloudinary service running 🚀");
});

// 🎬 MAIN MERGE ENDPOINT
app.post("/merge", async (req, res) => {
  try {
    const { clips } = req.body;

    if (!clips || clips.length < 2) {
      return res.status(400).json({ error: "Need at least 2 clips" });
    }

    const jobId = uuidv4();
    const tempDir = `/tmp/${jobId}`;
    fs.mkdirSync(tempDir);

    // ⬇️ 1. Download clips
    const localClips = [];

    for (let i = 0; i < clips.length; i++) {
      const url = clips[i];
      const filePath = `${tempDir}/clip_${i}.mp4`;

      const response = await axios({
        method: "GET",
        url,
        responseType: "stream",
      });

      const writer = fs.createWriteStream(filePath);
      response.data.pipe(writer);

      await new Promise((resolve, reject) => {
        writer.on("finish", resolve);
        writer.on("error", reject);
      });

      localClips.push(filePath);
    }

    // 🧾 2. Create concat file
    const concatPath = `${tempDir}/concat.txt`;
    const concatContent = localClips
      .map((c) => `file '${c}'`)
      .join("\n");

    fs.writeFileSync(concatPath, concatContent);

    // 🎥 3. Merge + compress
    const outputPath = `${tempDir}/output.mp4`;

    await new Promise((resolve, reject) => {
      exec(
        `ffmpeg -y -f concat -safe 0 -i ${concatPath} -vcodec libx264 -crf 28 -preset fast -acodec aac ${outputPath}`,
        (err) => {
          if (err) return reject(err);
          resolve();
        }
      );
    });

    // ☁️ 4. Upload to Cloudinary
    const uploadResult = await cloudinary.uploader.upload(outputPath, {
      resource_type: "video",
      folder: "ai-movies",
    });

    // 🧹 5. Cleanup
    fs.rmSync(tempDir, { recursive: true, force: true });

    // ✅ 6. Return URL
    res.json({
      success: true,
      url: uploadResult.secure_url,
      duration: uploadResult.duration,
    });

  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Processing failed",
      details: err.message,
    });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
