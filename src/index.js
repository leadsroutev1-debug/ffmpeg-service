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

const MAX_CLIPS = 50;
const MAX_CONCURRENT_JOBS = 2;
const DOWNLOAD_CONCURRENCY = 3;
const FFMPEG_TIMEOUT = 240000;

const OUTPUT_WIDTH = 480;
const OUTPUT_HEIGHT = 854;
const OUTPUT_FPS = 30;

const USE_GPU = process.env.USE_GPU === "true";

// Ken Burns zoom rate for the "zoom" composition layout. 0.0015/frame at
// 30fps ramps to ~1.5x zoom over roughly 11-12s -- tune if your shots run
// much longer or shorter than that.
const ZOOM_RATE = 0.0015;
const ZOOM_MAX = 1.5;

// How PIP boxes are sized/positioned for the "overlay" composition layout.
const OVERLAY_SCALE = 0.35; // overlay clip width as a fraction of the base
const OVERLAY_MARGIN = 24; // px from the edge

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

// Used to order clips: v1 relied on this for scene_NNN merge order; it also
// works fine for shot_NN compose order since both just end in a number.
const extractTrailingNumber = (str) => {
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

// Returns duration in seconds (float) via ffprobe. Needed by the "overlay"
// and "zoom" composition layouts, which have to reason about timing.
const getDuration = (file) => {
  return new Promise((resolve, reject) => {
    const ffprobe = spawn("ffprobe", [
      "-v", "error",
      "-show_entries", "format=duration",
      "-of", "default=noprint_wrappers=1:nokey=1",
      file
    ]);

    let out = "";
    ffprobe.stdout.on("data", d => { out += d.toString(); });
    ffprobe.on("close", code => {
      const val = parseFloat(out.trim());
      if (code !== 0 || Number.isNaN(val)) {
        return reject(new Error(`ffprobe failed for ${file} (exit ${code})`));
      }
      resolve(val);
    });
    ffprobe.on("error", reject);
  });
};

// ================= NORMALIZE =================
// Every clip -- whether it's headed into /merge or /compose -- gets forced
// to the same resolution/fps/audio format first, so every composition
// filter downstream (concat, overlay, vstack, zoompan) can assume matching
// inputs instead of guarding against mismatches itself.
const normalizeClip = async (input, output, jobId, webhook) => {
  await runFFmpeg([
    "-y",
    "-i", input,

    // ✅ ADD SILENT AUDIO TRACK (CRITICAL FIX) -- guarantees every clip has
    // an audio stream, even ones generated from a silent image-to-video job.
    "-f", "lavfi",
    "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",

    "-shortest",

    // ✅ SAFE SCALING (FIXES DIMENSION MISMATCH)
    "-vf", `scale=${OUTPUT_WIDTH}:${OUTPUT_HEIGHT}:force_original_aspect_ratio=decrease,pad=${OUTPUT_WIDTH}:${OUTPUT_HEIGHT}:(ow-iw)/2:(oh-ih)/2,fps=${OUTPUT_FPS}`,

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

// ================= COMPOSITION LAYOUTS =================
// Each compose function takes an array of already-normalized clip paths
// (same resolution/fps/audio format) and produces one output file. These
// are what /compose calls per scene, and "cut" is also what /merge uses to
// concatenate composed scenes into the final episode.

// cut: hard-cut concatenation, in order. Works for any number of clips.
const composeCut = async (normalized, outputPath, jobId, webhook) => {
  if (normalized.length === 1) {
    fs.copyFileSync(normalized[0], outputPath);
    return;
  }

  const filter =
    normalized.map((_, i) => `[${i}:v:0][${i}:a:0]`).join("") +
    `concat=n=${normalized.length}:v=1:a=1[outv][outa]`;

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
  ], jobId, webhook);
};

// overlay: normalized[0] plays as the full-frame base for its own duration.
// Every subsequent clip gets an equal-length window of that base's runtime
// as a small picture-in-picture box (bottom-right), one after another.
// Overlay clips are muted -- only the base clip's audio plays -- so we
// don't have to sync/mix multiple simultaneous dialogue tracks. If you need
// overlay audio too, mix it in with an amix graph; flagged here rather than
// guessed at, since the right mix levels are a creative call.
const composeOverlay = async (normalized, outputPath, jobId, webhook) => {
  const [base, ...overlays] = normalized;

  if (overlays.length === 0) {
    fs.copyFileSync(base, outputPath);
    return;
  }

  const baseDuration = await getDuration(base);
  const segDuration = baseDuration / overlays.length;
  const boxW = Math.round(OUTPUT_WIDTH * OVERLAY_SCALE);
  const boxH = Math.round(OUTPUT_HEIGHT * OVERLAY_SCALE);
  const x = OUTPUT_WIDTH - boxW - OVERLAY_MARGIN;
  const y = OUTPUT_HEIGHT - boxH - OVERLAY_MARGIN;

  const inputs = ["-i", base, ...overlays.flatMap(c => ["-i", c])];

  const scaleFilters = overlays
    .map((_, i) => `[${i + 1}:v]scale=${boxW}:${boxH}[ov${i}]`)
    .join(";");

  let chain = "[0:v]";
  const overlaySteps = overlays
    .map((_, i) => {
      const start = (i * segDuration).toFixed(3);
      const end = ((i + 1) * segDuration).toFixed(3);
      const inLabel = i === 0 ? "[0:v]" : `[tmp${i - 1}]`;
      const outLabel = i === overlays.length - 1 ? "[outv]" : `[tmp${i}]`;
      return `${inLabel}[ov${i}]overlay=${x}:${y}:enable='between(t,${start},${end})'${outLabel}`;
    })
    .join(";");

  const filter = `${scaleFilters};${overlaySteps}`;

  await runFFmpeg([
    "-y",
    ...inputs,
    "-filter_complex", filter,
    "-map", "[outv]",
    "-map", "0:a:0",
    "-t", baseDuration.toFixed(3),
    "-c:v", USE_GPU ? "h264_nvenc" : "libx264",
    "-preset", "veryfast",
    "-crf", "23",
    "-c:a", "aac",
    "-movflags", "+faststart",
    outputPath
  ], jobId, webhook);
};

// split: exactly 2 clips, stacked top/bottom (vstack) to suit the vertical
// 9:16 output -- side-by-side (hstack) would squeeze both shots too
// narrow at 480px wide. Both clips' audio is mixed together. If either
// clip is shorter than the other, the stack (and therefore the output)
// naturally ends when the shorter one runs out.
const composeSplit = async (normalized, outputPath, jobId, webhook) => {
  const [top, bottom] = normalized;
  const halfH = Math.floor(OUTPUT_HEIGHT / 2);

  const filter =
    `[0:v]scale=${OUTPUT_WIDTH}:${halfH}[top];` +
    `[1:v]scale=${OUTPUT_WIDTH}:${halfH}[bottom];` +
    `[top][bottom]vstack=inputs=2[outv];` +
    `[0:a][1:a]amix=inputs=2:duration=shortest:dropout_transition=0[outa]`;

  await runFFmpeg([
    "-y",
    "-i", top,
    "-i", bottom,
    "-filter_complex", filter,
    "-map", "[outv]",
    "-map", "[outa]",
    "-c:v", USE_GPU ? "h264_nvenc" : "libx264",
    "-preset", "veryfast",
    "-crf", "23",
    "-c:a", "aac",
    "-movflags", "+faststart",
    outputPath
  ], jobId, webhook);
};

// zoom: a single shot with a Ken Burns push-in. Only the first clip is
// used -- if more were supplied for a "zoom" scene, that's a Director/
// n8n-side mistake (a zoom scene should only ever have one shot), so we
// log it rather than silently discarding footage without a trace.
const composeZoom = async (normalized, outputPath, jobId, webhook) => {
  if (normalized.length > 1) {
    log("zoom_layout_extra_clips_ignored", { extraCount: normalized.length - 1 });
  }

  const clip = normalized[0];
  const duration = await getDuration(clip);
  const frames = Math.max(1, Math.round(duration * OUTPUT_FPS));

  const filter =
    `zoompan=z='min(zoom+${ZOOM_RATE},${ZOOM_MAX})':d=${frames}:` +
    `x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':` +
    `s=${OUTPUT_WIDTH}x${OUTPUT_HEIGHT}:fps=${OUTPUT_FPS}`;

  await runFFmpeg([
    "-y",
    "-i", clip,
    "-vf", filter,
    "-map", "0:a:0",
    "-c:v", USE_GPU ? "h264_nvenc" : "libx264",
    "-preset", "veryfast",
    "-crf", "23",
    "-c:a", "aac",
    "-movflags", "+faststart",
    outputPath
  ], jobId, webhook);
};

// Resolves the requested layout to a compose function, applying sane
// fallbacks (documented on the n8n side too) rather than failing the whole
// scene over a layout/clip-count mismatch.
const resolveComposer = (layout, clipCount) => {
  switch (layout) {
    case "split":
      if (clipCount !== 2) {
        log("split_layout_wrong_clip_count_falling_back_to_cut", { clipCount });
        return { fn: composeCut, usedLayout: "cut" };
      }
      return { fn: composeSplit, usedLayout: "split" };
    case "overlay":
      return { fn: composeOverlay, usedLayout: "overlay" };
    case "zoom":
      return { fn: composeZoom, usedLayout: "zoom" };
    case "cut":
    default:
      return { fn: composeCut, usedLayout: "cut" };
  }
};

// ================= SHARED JOB PIPELINE =================
// Both /merge and /compose do the same thing end to end -- queue, download,
// normalize, run one ffmpeg composition step, upload, report -- so that
// pipeline lives in one place and each route just supplies the ordering
// rule, the composer, and the upload folder.
const runClipJob = ({ requestId, clips, webhook, minClips, maxClips, sortClips, composerFor, uploadFolder }) => {
  jobLimit(async () => {
    let tempDir;

    try {
      await sendWebhook(webhook, { jobId: requestId, status: "started" });
      updateJob(requestId, { status: "processing" });

      if (!clips || clips.length < minClips) {
        throw new Error(`Need at least ${minClips} clip(s)`);
      }
      if (clips.length > maxClips) throw new Error("Too many clips");

      const ordered = sortClips ? [...clips].sort((a, b) => extractTrailingNumber(a) - extractTrailingNumber(b)) : clips;

      tempDir = path.join(os.tmpdir(), requestId);
      fs.mkdirSync(tempDir, { recursive: true });

      // ================= DOWNLOAD =================
      const limit = pLimit(DOWNLOAD_CONCURRENCY);
      const localClips = await Promise.all(
        ordered.map((url, i) =>
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

      updateJob(requestId, { step: "composing" });

      // ================= COMPOSE =================
      const { fn: composeFn, usedLayout } = composerFor(normalized.length);
      const outputPath = path.join(tempDir, "output.mp4");
      await composeFn(normalized, outputPath, requestId, webhook);

      updateJob(requestId, { step: "uploading" });

      log("upload_start", { path: outputPath, size: fs.statSync(outputPath).size });

      // ================= CLOUDINARY STREAM UPLOAD =================
      const upload = await new Promise((resolve, reject) => {
        const stream = cloudinary.uploader.upload_stream(
          { resource_type: "video", folder: uploadFolder },
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
        duration: upload.duration,
        layout: usedLayout
      };

      jobs.set(requestId, { ...jobs.get(requestId), ...result });
      await sendWebhook(webhook, result);

    } catch (err) {
      const failPayload = {
        jobId: requestId,
        status: "failed",
        error: err?.response?.data || err.message
      };

      jobs.set(requestId, { ...jobs.get(requestId), ...failPayload });
      await sendWebhook(webhook, failPayload);

      if (tempDir) fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });
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

// ================= MERGE (final episode: composed scene clips -> one video) =================
app.post("/merge", auth, (req, res) => {
  const requestId = uuidv4();
  jobs.set(requestId, { id: requestId, status: "queued", progress: 0 });

  const { clips, webhook } = req.body;

  runClipJob({
    requestId,
    clips,
    webhook,
    minClips: 2,
    maxClips: MAX_CLIPS,
    sortClips: true, // scene_NNN URLs -- keep numeric-order safety net from v1
    composerFor: () => ({ fn: composeCut, usedLayout: "cut" }),
    uploadFolder: "ai-movies/episodes",
  });

  res.json({ jobId: requestId, statusUrl: `/status/${requestId}` });
});

// ================= COMPOSE (one scene: shot clips -> one scene clip) =================
// New endpoint required by the multiverse/variant n8n workflow. Body:
//   { clips: string[], layout: "cut" | "overlay" | "split" | "zoom", webhook? }
// Returns { jobId, statusUrl } immediately, same shape as /merge; poll
// /status/:id for { status: "processing" | "done" | "failed", url, layout }.
app.post("/compose", auth, (req, res) => {
  const requestId = uuidv4();
  jobs.set(requestId, { id: requestId, status: "queued", progress: 0 });

  const { clips, webhook } = req.body;
  const layout = (req.body.layout || "cut").toLowerCase();

  if (!["cut", "overlay", "split", "zoom"].includes(layout)) {
    jobs.set(requestId, {
      id: requestId,
      status: "failed",
      error: `Unknown layout '${layout}'. Expected cut, overlay, split, or zoom.`
    });
    return res.status(400).json({ error: "Invalid layout" });
  }

  runClipJob({
    requestId,
    clips,
    webhook,
    // shot clips arrive already ordered (shot_01, shot_02, ...) from the
    // n8n side's Cloudinary folder listing; re-sorting is a safety net,
    // same principle as /merge trusting scene_NNN numbering.
    minClips: layout === "zoom" ? 1 : 2,
    maxClips: MAX_CLIPS,
    sortClips: true,
    composerFor: (clipCount) => resolveComposer(layout, clipCount),
    uploadFolder: "ai-movies/scenes",
  });

  res.json({ jobId: requestId, statusUrl: `/status/${requestId}` });
});

// ================= START =================
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
