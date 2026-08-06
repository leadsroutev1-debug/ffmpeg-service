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

// ✅ MEMORY FIX: this was 2. Two concurrent jobs means two full ffmpeg
// pipelines (normalize + effects + compose, each its own ffmpeg process)
// running at once, which roughly doubles peak RSS. On a small Render
// instance that's very likely what's tipping you into OOM. Default to
// running jobs one at a time; bump via env if you upgrade the instance.
const MAX_CONCURRENT_JOBS = parseInt(process.env.MAX_CONCURRENT_JOBS || "1", 10);

// ✅ MEMORY FIX: fewer simultaneous downloads = less concurrent disk-write
// buffering and fewer open sockets/streams at once.
const DOWNLOAD_CONCURRENCY = parseInt(process.env.DOWNLOAD_CONCURRENCY || "2", 10);

const FFMPEG_TIMEOUT = parseInt(process.env.FFMPEG_TIMEOUT || "120000", 10);

const OUTPUT_WIDTH = 480;
const OUTPUT_HEIGHT = 854;
const OUTPUT_FPS = 30;

const USE_GPU = process.env.USE_GPU === "true";

// ✅ MEMORY FIX: cap ffmpeg's own thread count. Each thread the encoder
// spins up gets its own frame buffers/lookahead window, so on a
// memory-constrained box (as opposed to CPU-constrained), fewer threads
// means lower peak RSS per ffmpeg process, at the cost of some speed.
const FFMPEG_THREADS = parseInt(process.env.FFMPEG_THREADS || "1", 10);

// ✅ MEMORY FIX: libx264's biggest memory levers are B-frame reordering
// and rc-lookahead (the encoder buffers this many future frames to plan
// bitrate). Both scale with resolution × frame count held in memory.
// At 480x854 this is a small win per-process, but it compounds across
// every clip you normalize/effect/compose. Override via env if quality
// suffers and you have RAM to spare.
const X264_LOW_MEM_PARAMS = process.env.X264_LOW_MEM_PARAMS ||
  "rc-lookahead=10:ref=1:bframes=0";

// ✅ IMPORTANT (read this): os.tmpdir() usually resolves to /tmp. On many
// container platforms -- Render included, depending on plan/runtime --
// /tmp is a tmpfs mount, meaning it's backed by RAM, not disk. If that's
// the case here, every downloaded clip + every normalized/effect/compose
// intermediate file is *also* counting against your memory limit, on top
// of ffmpeg's own working memory. That would explain OOM specifically
// during "stitching" (many clips × multiple copies of each on disk at
// once). Set TMP_DIR to a real persistent disk mount (Render lets you
// attach one in the dashboard) to rule this out / fix it for good.
const TMP_ROOT = process.env.TMP_DIR || os.tmpdir();

// ✅ MEMORY FIX (leak, not spike): the in-memory `jobs` Map never had
// anything removed from it, so on a long-running instance it grows
// forever and slowly eats RAM until something (maybe this) tips you over.
// Finished jobs are now swept out after JOB_TTL_MS.
const JOB_TTL_MS = parseInt(process.env.JOB_TTL_MS || String(60 * 60 * 1000), 10); // 1h

// Cloudinary's plain `upload_stream` is a single non-chunked multipart
// request, capped at 100MB on most plans. Chunked upload (upload_large)
// streams the file in CLOUDINARY_CHUNK_SIZE-byte pieces instead.
const CLOUDINARY_CHUNK_SIZE = parseInt(process.env.CLOUDINARY_CHUNK_SIZE || "20000000", 10); // 20MB default
const CLOUDINARY_UPLOAD_TIMEOUT = parseInt(process.env.CLOUDINARY_UPLOAD_TIMEOUT || "600000", 10); // 10 min

// Ken Burns zoom rate for the "zoom" composition layout.
const ZOOM_RATE = 0.0015;
const ZOOM_MAX = 1.5;

// PIP box sizing/positioning for the "overlay" composition layout.
const OVERLAY_SCALE = 0.35;
const OVERLAY_MARGIN = 24;

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

// ✅ MEMORY FIX: periodic sweep of completed/failed jobs older than
// JOB_TTL_MS so the Map doesn't grow unbounded over the service's uptime.
setInterval(() => {
  const now = Date.now();
  for (const [id, job] of jobs.entries()) {
    if (
      (job.status === "done" || job.status === "failed") &&
      job._completedAt &&
      now - job._completedAt > JOB_TTL_MS
    ) {
      jobs.delete(id);
    }
  }
}, 5 * 60 * 1000);

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
// ✅ MEMORY FIX: every ffmpeg invocation now gets a global "-threads N"
// cap injected right after "-y" (before any -i), which bounds both decode
// and filter-graph thread pools, not just the encoder.
const withThreadCap = (args) => ["-y", "-threads", String(FFMPEG_THREADS), ...args.filter(a => a !== "-y")];

const runFFmpeg = (rawArgs, jobId, webhook) => {
  const args = withThreadCap(rawArgs);
  return new Promise((resolve, reject) => {
    const ffmpeg = spawn("ffmpeg", args);

    // ✅ DIAGNOSTIC FIX: a SIGKILL from our own FFMPEG_TIMEOUT and a
    // SIGKILL from the OS OOM killer look identical on close (code: null,
    // signal: "SIGKILL") -- there was no way to tell them apart from the
    // logs. `timedOut` disambiguates: if it's true, this was our timeout;
    // if close still reports SIGKILL/null with timedOut false, that's a
    // strong signal it was killed externally (most likely OOM).
    let timedOut = false;
    let lastProgress = null;

    const timeout = setTimeout(() => {
      timedOut = true;
      log("ffmpeg_timeout_killing", { jobId, timeoutMs: FFMPEG_TIMEOUT, lastProgress });
      ffmpeg.kill("SIGKILL");
    }, FFMPEG_TIMEOUT);

    ffmpeg.stderr.on("data", async (d) => {
      const output = d.toString();
      log("ffmpeg", { output });

      const timeMatch = output.match(/time=(\d+:\d+:\d+\.\d+)/);
      if (timeMatch) {
        const progress = timeMatch[1];
        lastProgress = progress;

        updateJob(jobId, { progress });

        await sendWebhook(webhook, {
          jobId,
          status: "processing",
          progress
        });
      }
    });

    ffmpeg.on("close", (code, signal) => {
      clearTimeout(timeout);

      if (timedOut) {
        log("ffmpeg_failed_timeout", { jobId, timeoutMs: FFMPEG_TIMEOUT, lastProgress });
        return reject(new Error(`FFmpeg timeout after ${FFMPEG_TIMEOUT}ms (last progress: ${lastProgress || "none"})`));
      }

      if (code !== 0) {
        // code: null + signal: "SIGKILL"/"SIGTERM" here (with timedOut
        // false) most likely means something outside this process killed
        // it -- on Render that's almost always the OOM killer.
        log("ffmpeg_failed", { code, signal, lastProgress, likelyOOM: code === null && !!signal });
        return reject(new Error(`FFmpeg exit code=${code} signal=${signal || "none"}`));
      }
      resolve();
    });

    ffmpeg.on("error", err => {
      clearTimeout(timeout);
      reject(err);
    });
  });
};

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

const hasAudioStream = (file) => {
  return new Promise((resolve) => {
    const ffprobe = spawn("ffprobe", [
      "-v", "error",
      "-select_streams", "a",
      "-show_entries", "stream=codec_type",
      "-of", "default=noprint_wrappers=1:nokey=1",
      file
    ]);

    let out = "";
    ffprobe.stdout.on("data", d => { out += d.toString(); });
    ffprobe.on("close", () => resolve(out.trim() === "audio"));
    ffprobe.on("error", () => resolve(false));
  });
};

// ✅ Shared encode-arg builder so the thread cap + low-memory x264 params
// live in exactly one place instead of being repeated (and easy to forget)
// across every compose/effects function.
const videoEncodeArgs = () => {
  if (USE_GPU) {
    return ["-c:v", "h264_nvenc", "-preset", "p3"];
  }
  return [
    "-c:v", "libx264",
    "-preset", "veryfast",
    "-crf", "23",
    "-x264-params", X264_LOW_MEM_PARAMS
  ];
};

// ================= NORMALIZE =================
const normalizeClip = async (input, output, jobId, webhook) => {
  const hasAudio = await hasAudioStream(input);

  const args = ["-i", input];

  if (!hasAudio) {
    args.push(
      "-f", "lavfi",
      "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
      "-shortest"
    );
  }

  args.push(
    "-vf", `scale=${OUTPUT_WIDTH}:${OUTPUT_HEIGHT}:force_original_aspect_ratio=decrease,pad=${OUTPUT_WIDTH}:${OUTPUT_HEIGHT}:(ow-iw)/2:(oh-ih)/2,fps=${OUTPUT_FPS}`,
    ...videoEncodeArgs(),
    "-c:a", "aac",
    "-ar", "44100",
    "-ac", "2",
    "-movflags", "+faststart",
    output
  );

  await runFFmpeg(args, jobId, webhook);
};

// ================= CONCAT (fast path) =================
// ✅ MEMORY FIX: this is the single biggest change. Every clip passed to
// a compose step has already been through normalizeClip with IDENTICAL
// codec/resolution/fps/audio settings. That means for a plain hard-cut
// with no transition and no per-clip effects, we don't need to decode
// and re-encode anything at all -- the concat *demuxer* can just splice
// the already-encoded bitstreams together with "-c copy". The old
// filter_complex concat had to keep every single input's decoder open
// simultaneously to feed the filter graph, so peak memory scaled with
// clip count (50 clips = 50 open decoders). This fast path's memory
// footprint is flat regardless of how many clips are in the job.
const concatDemuxerCopy = async (normalized, outputPath, jobId, webhook) => {
  const listFile = `${outputPath}.concat.txt`;
  const listContent = normalized
    .map(f => `file '${path.resolve(f).replace(/'/g, "'\\''")}'`)
    .join("\n");
  fs.writeFileSync(listFile, listContent);

  try {
    await runFFmpeg([
      "-f", "concat",
      "-safe", "0",
      "-i", listFile,
      "-c", "copy",
      "-movflags", "+faststart",
      outputPath
    ], jobId, webhook);
  } finally {
    fs.rmSync(listFile, { force: true });
  }
};

// ================= COMPOSITION LAYOUTS =================
const composeCut = async (normalized, outputPath, jobId, webhook) => {
  if (normalized.length === 1) {
    fs.copyFileSync(normalized[0], outputPath);
    return;
  }

  try {
    await concatDemuxerCopy(normalized, outputPath, jobId, webhook);
    return;
  } catch (err) {
    // Falls back to the old decode+re-encode path only if stream-copy
    // concat fails (e.g. a clip slipped through with mismatched params).
    log("concat_demuxer_failed_falling_back_to_filter_complex", { error: err.message });
  }

  const filter =
    normalized.map((_, i) => `[${i}:v:0][${i}:a:0]`).join("") +
    `concat=n=${normalized.length}:v=1:a=1[outv][outa]`;

  await runFFmpeg([
    ...normalized.flatMap(c => ["-i", c]),
    "-filter_complex", filter,
    "-map", "[outv]",
    "-map", "[outa]",
    ...videoEncodeArgs(),
    "-c:a", "aac",
    "-movflags", "+faststart",
    outputPath
  ], jobId, webhook);
};

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
    ...inputs,
    "-filter_complex", filter,
    "-map", "[outv]",
    "-map", "0:a:0",
    "-t", baseDuration.toFixed(3),
    ...videoEncodeArgs(),
    "-c:a", "aac",
    "-movflags", "+faststart",
    outputPath
  ], jobId, webhook);
};

const composeSplit = async (normalized, outputPath, jobId, webhook) => {
  const [top, bottom] = normalized;
  const halfH = Math.floor(OUTPUT_HEIGHT / 2);

  const filter =
    `[0:v]scale=${OUTPUT_WIDTH}:${halfH}[top];` +
    `[1:v]scale=${OUTPUT_WIDTH}:${halfH}[bottom];` +
    `[top][bottom]vstack=inputs=2[outv];` +
    `[0:a][1:a]amix=inputs=2:duration=shortest:dropout_transition=0[outa]`;

  await runFFmpeg([
    "-i", top,
    "-i", bottom,
    "-filter_complex", filter,
    "-map", "[outv]",
    "-map", "[outa]",
    ...videoEncodeArgs(),
    "-c:a", "aac",
    "-movflags", "+faststart",
    outputPath
  ], jobId, webhook);
};

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
    "-i", clip,
    "-vf", filter,
    "-map", "0:a:0",
    ...videoEncodeArgs(),
    "-c:a", "aac",
    "-movflags", "+faststart",
    outputPath
  ], jobId, webhook);
};

// ============================================================
// ================= STUDIO EFFECTS CATALOG ====================
// ============================================================
const TRANSITIONS = [
  "fade", "fadeblack", "fadewhite", "distance",
  "wipeleft", "wiperight", "wipeup", "wipedown",
  "slideleft", "slideright", "slideup", "slidedown",
  "smoothleft", "smoothright", "smoothup", "smoothdown",
  "circlecrop", "rectcrop", "circleopen", "circleclose",
  "vertopen", "vertclose", "horzopen", "horzclose",
  "dissolve", "pixelize",
  "diagtl", "diagtr", "diagbl", "diagbr",
  "hlslice", "hrslice", "vuslice", "vdslice",
  "hblur", "fadegrays",
  "wipetl", "wipetr", "wipebl", "wipebr",
  "squeezeh", "squeezev", "zoomin",
  "hlwind", "hrwind", "vuwind", "vdwind",
  "coverleft", "coverright", "coverup", "coverdown",
  "revealleft", "revealright", "revealup", "revealdown"
];

const MOTION_EFFECTS = {
  zoom_in: (dur) => {
    const frames = Math.max(1, Math.round(dur * OUTPUT_FPS));
    return { vf: `zoompan=z='min(zoom+${ZOOM_RATE},${ZOOM_MAX})':d=${frames}:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s=${OUTPUT_WIDTH}x${OUTPUT_HEIGHT}:fps=${OUTPUT_FPS}` };
  },
  zoom_out: (dur) => {
    const frames = Math.max(1, Math.round(dur * OUTPUT_FPS));
    return { vf: `zoompan=z='if(eq(on,0),${ZOOM_MAX},max(zoom-${ZOOM_RATE},1))':d=${frames}:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s=${OUTPUT_WIDTH}x${OUTPUT_HEIGHT}:fps=${OUTPUT_FPS}` };
  },
  kenburns_left: (dur) => ({
    vf: `scale=${Math.round(OUTPUT_WIDTH * 1.15)}:${Math.round(OUTPUT_HEIGHT * 1.15)},crop=${OUTPUT_WIDTH}:${OUTPUT_HEIGHT}:x='(iw-ow)*(1-t/${Math.max(dur, 0.1).toFixed(3)})':y='(ih-oh)/2'`
  }),
  kenburns_right: (dur) => ({
    vf: `scale=${Math.round(OUTPUT_WIDTH * 1.15)}:${Math.round(OUTPUT_HEIGHT * 1.15)},crop=${OUTPUT_WIDTH}:${OUTPUT_HEIGHT}:x='(iw-ow)*(t/${Math.max(dur, 0.1).toFixed(3)})':y='(ih-oh)/2'`
  }),
  pan_left: (dur) => ({
    vf: `scale=${Math.round(OUTPUT_WIDTH * 1.12)}:${OUTPUT_HEIGHT},crop=${OUTPUT_WIDTH}:${OUTPUT_HEIGHT}:x='(iw-ow)*(1-t/${Math.max(dur, 0.1).toFixed(3)})':y=0`
  }),
  pan_right: (dur) => ({
    vf: `scale=${Math.round(OUTPUT_WIDTH * 1.12)}:${OUTPUT_HEIGHT},crop=${OUTPUT_WIDTH}:${OUTPUT_HEIGHT}:x='(iw-ow)*(t/${Math.max(dur, 0.1).toFixed(3)})':y=0`
  }),
  pan_up: (dur) => ({
    vf: `scale=${OUTPUT_WIDTH}:${Math.round(OUTPUT_HEIGHT * 1.12)},crop=${OUTPUT_WIDTH}:${OUTPUT_HEIGHT}:x=0:y='(ih-oh)*(1-t/${Math.max(dur, 0.1).toFixed(3)})'`
  }),
  pan_down: (dur) => ({
    vf: `scale=${OUTPUT_WIDTH}:${Math.round(OUTPUT_HEIGHT * 1.12)},crop=${OUTPUT_WIDTH}:${OUTPUT_HEIGHT}:x=0:y='(ih-oh)*(t/${Math.max(dur, 0.1).toFixed(3)})'`
  }),
  shake_handheld: () => ({
    vf: `scale=${OUTPUT_WIDTH + 24}:${OUTPUT_HEIGHT + 24},crop=${OUTPUT_WIDTH}:${OUTPUT_HEIGHT}:x='12+6*sin(2*PI*t*2)':y='12+6*cos(2*PI*t*1.7)'`
  }),
  rotate_slight: () => ({
    vf: `rotate='(2*PI/180)*sin(2*PI*t/4)':ow=iw:oh=ih:fillcolor=black`
  }),
  dutch_angle_left: () => ({
    vf: `rotate=-6*PI/180:ow=iw:oh=ih:fillcolor=black`
  }),
  dutch_angle_right: () => ({
    vf: `rotate=6*PI/180:ow=iw:oh=ih:fillcolor=black`
  }),
  slow_motion_2x: () => ({
    vf: `setpts=2.0*PTS`, af: `atempo=0.5`
  }),
  slow_motion_4x: () => ({
    vf: `setpts=4.0*PTS`, af: `atempo=0.5,atempo=0.5`
  }),
  fast_forward_2x: () => ({
    vf: `setpts=0.5*PTS`, af: `atempo=2.0`
  }),
  fast_forward_4x: () => ({
    vf: `setpts=0.25*PTS`, af: `atempo=2.0,atempo=2.0`
  }),
  reverse_clip: () => ({
    vf: `reverse`, af: `areverse`
  }),
  mirror_flip: () => ({ vf: `hflip` }),
  vertical_flip: () => ({ vf: `vflip` })
};

const COLOR_GRADES = {
  cinematic_teal_orange: `colorbalance=rs=-0.12:gs=0.03:bs=0.15:rm=-0.06:bm=0.1:rh=0.08:bh=-0.05,eq=contrast=1.12:saturation=1.1`,
  noir_bw: `hue=s=0,eq=contrast=1.3:brightness=-0.02`,
  sepia_vintage: `colorchannelmixer=.393:.769:.189:0:.349:.686:.168:0:.272:.534:.131`,
  bleach_bypass: `eq=saturation=0.4:contrast=1.3`,
  high_contrast: `eq=contrast=1.5`,
  desaturated: `eq=saturation=0.3`,
  warm_golden: `colorbalance=rm=0.15:gm=0.05:bm=-0.1`,
  cool_blue: `colorbalance=bm=0.2:rm=-0.1`,
  vintage_curve: `curves=preset=vintage`,
  cross_process: `curves=preset=cross_process`,
  strong_contrast: `curves=preset=strong_contrast`,
  negative: `curves=preset=negative`,
  color_negative: `curves=preset=color_negative`,
  darker: `curves=preset=darker`,
  lighter: `curves=preset=lighter`,
  linear_contrast: `curves=preset=linear_contrast`,
  medium_contrast: `curves=preset=medium_contrast`,
  increase_contrast: `curves=preset=increase_contrast`,
  dreamy_soft: `gblur=sigma=1.5,eq=brightness=0.04:contrast=0.95`,
  moody_dark: `eq=brightness=-0.08:contrast=1.2:saturation=0.7`,
  technicolor: `eq=saturation=1.8:contrast=1.2`,
  infrared_look: `colorchannelmixer=0:0:1:0:0:1:0:0:1:0:0:0`,
  matrix_green: `colorchannelmixer=0.1:0.9:0:0:0:0.9:0.1:0:0.1:0:0.8:0`,
  faded_polaroid: `curves=preset=vintage,eq=contrast=0.9:brightness=0.05`,
  sunset_glow: `colorbalance=rm=0.2:gm=0.05,eq=brightness=0.03`,
  horror_green: `colorbalance=gm=0.3:rm=-0.2,eq=contrast=1.15`
};

const OVERLAY_EFFECTS = {
  vignette_dark: `vignette=PI/4`,
  film_grain: `noise=alls=20:allf=t+u`,
  dust_scratches: `noise=alls=8:allf=t+u`,
  vhs_overlay: `noise=alls=10:allf=t,rgbashift=rh=1:bh=-1`,
  scanlines: `drawgrid=width=iw:height=4:thickness=1:color=black@0.25`,
  letterbox_cinematic: `drawbox=x=0:y=0:w=iw:h=ih*0.12:color=black@1:t=fill,drawbox=x=0:y=ih*0.88:w=iw:h=ih*0.12:color=black@1:t=fill`,
  chromatic_aberration: `rgbashift=rh=2:bh=-2`,
  light_leak_warm: `vignette=PI/6:mode=backward,eq=brightness=0.03:saturation=1.1`
};

const escapeDrawtext = (text) =>
  String(text).replace(/\\/g, "\\\\").replace(/:/g, "\\:").replace(/'/g, "\\'");

const buildDrawText = ({ text, position = "bottom", fontSize = 36, fontColor = "white" }) => {
  const safe = escapeDrawtext(text);
  const yExpr =
    position === "top" ? "40" :
    position === "center" ? "(h-text_h)/2" :
    "h-text_h-40";
  return `drawtext=text='${safe}':fontcolor=${fontColor}:fontsize=${fontSize}:box=1:boxcolor=black@0.5:boxborderw=10:x=(w-text_w)/2:y=${yExpr}`;
};

const buildEffectFilterChain = (spec, duration) => {
  const vfParts = [];
  const afParts = [];

  if (spec?.motion) {
    const fn = MOTION_EFFECTS[spec.motion];
    if (fn) {
      const { vf, af } = fn(duration);
      if (vf) vfParts.push(vf);
      if (af) afParts.push(af);
    } else {
      log("unknown_motion_effect_skipped", { name: spec.motion });
    }
  }

  if (spec?.colorGrade) {
    const grade = COLOR_GRADES[spec.colorGrade];
    if (grade) vfParts.push(grade);
    else log("unknown_color_grade_skipped", { name: spec.colorGrade });
  }

  if (Array.isArray(spec?.overlays)) {
    for (const name of spec.overlays) {
      const ov = OVERLAY_EFFECTS[name];
      if (ov) vfParts.push(ov);
      else log("unknown_overlay_skipped", { name });
    }
  }

  return { vf: vfParts.join(","), af: afParts.join(",") };
};

const applyClipEffects = async (input, output, spec, jobId, webhook) => {
  const hasEffects = spec && (spec.motion || spec.colorGrade || (spec.overlays && spec.overlays.length));
  if (!hasEffects) {
    fs.copyFileSync(input, output);
    return;
  }

  const duration = await getDuration(input);
  const { vf, af } = buildEffectFilterChain(spec, duration);

  const args = ["-i", input];
  if (vf) args.push("-vf", vf);
  args.push(...videoEncodeArgs());
  if (af) {
    args.push("-af", af, "-c:a", "aac");
  } else {
    args.push("-c:a", "copy");
  }
  args.push("-movflags", "+faststart", output);

  await runFFmpeg(args, jobId, webhook);
};

// ✅ MEMORY FIX: rolling pairwise merge instead of one filter_complex
// graph with all N clips open at once. Each step only ever has 2 inputs
// decoding simultaneously, so memory stays flat no matter how many clips
// are in the scene/episode. Intermediate step files are written to disk
// and deleted as soon as they're consumed by the next step.
const composeCutTransition = async (normalized, outputPath, jobId, webhook, transitionName, transitionDuration) => {
  if (normalized.length === 1) {
    fs.copyFileSync(normalized[0], outputPath);
    return;
  }

  const tmpDir = path.dirname(outputPath);
  let current = normalized[0];
  let ownsCurrent = false; // true once "current" is a step file we created (safe to delete)

  for (let i = 1; i < normalized.length; i++) {
    const next = normalized[i];
    const currentDuration = await getDuration(current);
    const nextDuration = await getDuration(next);
    const td = Math.max(0.1, Math.min(transitionDuration || 0.5, Math.min(currentDuration, nextDuration) / 2));
    const offset = Math.max(0, currentDuration - td);
    const isLast = i === normalized.length - 1;
    const stepOut = isLast ? outputPath : path.join(tmpDir, `xfstep_${i}.mp4`);

    const filter =
      `[0:v][1:v]xfade=transition=${transitionName}:duration=${td.toFixed(3)}:offset=${offset.toFixed(3)}[outv];` +
      `[0:a][1:a]acrossfade=d=${td.toFixed(3)}[outa]`;

    await runFFmpeg([
      "-i", current,
      "-i", next,
      "-filter_complex", filter,
      "-map", "[outv]",
      "-map", "[outa]",
      ...videoEncodeArgs(),
      "-c:a", "aac",
      "-movflags", "+faststart",
      stepOut
    ], jobId, webhook);

    if (ownsCurrent) fs.rmSync(current, { force: true });
    current = stepOut;
    ownsCurrent = true;
  }
};

const composeCutOrTransition = async (normalized, outputPath, jobId, webhook, opts = {}) => {
  if (opts.transition && TRANSITIONS.includes(opts.transition) && normalized.length > 1) {
    return composeCutTransition(normalized, outputPath, jobId, webhook, opts.transition, opts.transitionDuration);
  }
  return composeCut(normalized, outputPath, jobId, webhook);
};

const composeGrid4 = async (normalized, outputPath, jobId, webhook) => {
  const halfW = Math.floor(OUTPUT_WIDTH / 2);
  const halfH = Math.floor(OUTPUT_HEIGHT / 2);

  const filter =
    `[0:v]scale=${halfW}:${halfH}[tl];` +
    `[1:v]scale=${halfW}:${halfH}[tr];` +
    `[2:v]scale=${halfW}:${halfH}[bl];` +
    `[3:v]scale=${halfW}:${halfH}[br];` +
    `[tl][tr]hstack=inputs=2[top];` +
    `[bl][br]hstack=inputs=2[bottom];` +
    `[top][bottom]vstack=inputs=2[outv];` +
    `[0:a][1:a][2:a][3:a]amix=inputs=4:duration=shortest:dropout_transition=0[outa]`;

  await runFFmpeg([
    ...normalized.slice(0, 4).flatMap(c => ["-i", c]),
    "-filter_complex", filter,
    "-map", "[outv]",
    "-map", "[outa]",
    ...videoEncodeArgs(),
    "-c:a", "aac",
    "-movflags", "+faststart",
    outputPath
  ], jobId, webhook);
};

const composeTriptych = async (normalized, outputPath, jobId, webhook) => {
  const colW = Math.floor(OUTPUT_WIDTH / 3);

  const filter =
    `[0:v]scale=${colW}:${OUTPUT_HEIGHT}[a];` +
    `[1:v]scale=${colW}:${OUTPUT_HEIGHT}[b];` +
    `[2:v]scale=${OUTPUT_WIDTH - colW * 2}:${OUTPUT_HEIGHT}[c];` +
    `[a][b][c]hstack=inputs=3[outv];` +
    `[0:a][1:a][2:a]amix=inputs=3:duration=shortest:dropout_transition=0[outa]`;

  await runFFmpeg([
    ...normalized.slice(0, 3).flatMap(c => ["-i", c]),
    "-filter_complex", filter,
    "-map", "[outv]",
    "-map", "[outa]",
    ...videoEncodeArgs(),
    "-c:a", "aac",
    "-movflags", "+faststart",
    outputPath
  ], jobId, webhook);
};

const composePipCustom = async (normalized, outputPath, jobId, webhook, opts = {}) => {
  const [base, pip] = normalized;
  const scale = opts.pipScale || OVERLAY_SCALE;
  const boxW = Math.round(OUTPUT_WIDTH * scale);
  const boxH = Math.round(OUTPUT_HEIGHT * scale);
  const corner = opts.pipCorner || "bottom-right";

  const positions = {
    "top-left": [OVERLAY_MARGIN, OVERLAY_MARGIN],
    "top-right": [OUTPUT_WIDTH - boxW - OVERLAY_MARGIN, OVERLAY_MARGIN],
    "bottom-left": [OVERLAY_MARGIN, OUTPUT_HEIGHT - boxH - OVERLAY_MARGIN],
    "bottom-right": [OUTPUT_WIDTH - boxW - OVERLAY_MARGIN, OUTPUT_HEIGHT - boxH - OVERLAY_MARGIN],
    "center": [Math.round((OUTPUT_WIDTH - boxW) / 2), Math.round((OUTPUT_HEIGHT - boxH) / 2)]
  };
  const [x, y] = positions[corner] || positions["bottom-right"];

  const useAmix = !!opts.pipAudio;
  const filter = useAmix
    ? `[1:v]scale=${boxW}:${boxH}[pv];[0:v][pv]overlay=${x}:${y}[outv];[0:a][1:a]amix=inputs=2:duration=first:dropout_transition=0[outa]`
    : `[1:v]scale=${boxW}:${boxH}[pv];[0:v][pv]overlay=${x}:${y}[outv]`;

  await runFFmpeg([
    "-i", base,
    "-i", pip,
    "-filter_complex", filter,
    "-map", "[outv]",
    "-map", useAmix ? "[outa]" : "0:a",
    ...videoEncodeArgs(),
    "-c:a", "aac",
    "-movflags", "+faststart",
    outputPath
  ], jobId, webhook);
};

const buildFinalFilterChain = (opts = {}) => {
  const vfParts = [];
  if (opts.finalColorGrade && COLOR_GRADES[opts.finalColorGrade]) {
    vfParts.push(COLOR_GRADES[opts.finalColorGrade]);
  } else if (opts.finalColorGrade) {
    log("unknown_final_color_grade_skipped", { name: opts.finalColorGrade });
  }
  if (Array.isArray(opts.finalOverlays)) {
    for (const name of opts.finalOverlays) {
      const ov = OVERLAY_EFFECTS[name];
      if (ov) vfParts.push(ov);
      else log("unknown_final_overlay_skipped", { name });
    }
  }
  if (opts.textOverlay?.text) {
    vfParts.push(buildDrawText(opts.textOverlay));
  }
  return vfParts.join(",");
};

const applyFinalEffects = async (input, output, opts, jobId, webhook) => {
  const chain = buildFinalFilterChain(opts);
  if (!chain) {
    fs.copyFileSync(input, output);
    return;
  }
  await runFFmpeg([
    "-i", input,
    "-vf", chain,
    ...videoEncodeArgs(),
    "-c:a", "copy",
    "-movflags", "+faststart",
    output
  ], jobId, webhook);
};

const resolveComposer = (layout, clipCount, opts = {}) => {
  switch (layout) {
    case "split":
      if (clipCount !== 2) {
        log("split_layout_wrong_clip_count_falling_back_to_cut", { clipCount });
        return { fn: (n, o, j, w) => composeCutOrTransition(n, o, j, w, opts), usedLayout: "cut" };
      }
      return { fn: composeSplit, usedLayout: "split" };
    case "overlay":
      return { fn: composeOverlay, usedLayout: "overlay" };
    case "zoom":
      return { fn: composeZoom, usedLayout: "zoom" };
    case "grid4":
      if (clipCount !== 4) {
        log("grid4_layout_wrong_clip_count_falling_back_to_cut", { clipCount });
        return { fn: (n, o, j, w) => composeCutOrTransition(n, o, j, w, opts), usedLayout: "cut" };
      }
      return { fn: composeGrid4, usedLayout: "grid4" };
    case "triptych":
      if (clipCount !== 3) {
        log("triptych_layout_wrong_clip_count_falling_back_to_cut", { clipCount });
        return { fn: (n, o, j, w) => composeCutOrTransition(n, o, j, w, opts), usedLayout: "cut" };
      }
      return { fn: composeTriptych, usedLayout: "triptych" };
    case "pip":
      if (clipCount !== 2) {
        log("pip_layout_wrong_clip_count_falling_back_to_cut", { clipCount });
        return { fn: (n, o, j, w) => composeCutOrTransition(n, o, j, w, opts), usedLayout: "cut" };
      }
      return { fn: (n, o, j, w) => composePipCustom(n, o, j, w, opts), usedLayout: "pip" };
    case "cut":
    default:
      return { fn: (n, o, j, w) => composeCutOrTransition(n, o, j, w, opts), usedLayout: "cut" };
  }
};

const LAYOUTS = ["cut", "overlay", "split", "zoom", "grid4", "triptych", "pip"];

const parseClipEntry = (entry) => {
  if (typeof entry === "string") return { url: entry, effects: null };
  return {
    url: entry.url,
    effects: {
      motion: entry.motion || null,
      colorGrade: entry.colorGrade || null,
      overlays: Array.isArray(entry.overlays) ? entry.overlays : []
    }
  };
};

// ================= CLOUDINARY UPLOAD =================
const uploadToCloudinary = (outputPath, uploadFolder) => {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(outputPath)) {
      return reject(new Error(`Upload source missing: ${outputPath}`));
    }
    const size = fs.statSync(outputPath).size;
    if (size === 0) {
      return reject(new Error(`Upload source is empty: ${outputPath}`));
    }

    log("cloudinary_upload_start", { path: outputPath, sizeBytes: size, folder: uploadFolder });

    cloudinary.uploader.upload_large(
      outputPath,
      {
        resource_type: "video",
        folder: uploadFolder,
        chunk_size: CLOUDINARY_CHUNK_SIZE,
        timeout: CLOUDINARY_UPLOAD_TIMEOUT
      },
      (err, result) => {
        if (err) {
          log("cloudinary_upload_failed", {
            error: err.message,
            http_code: err.http_code,
            name: err.name
          });
          return reject(err);
        }
        log("cloudinary_upload_done", { url: result.secure_url, bytes: result.bytes });
        resolve(result);
      }
    );
  });
};

// ================= SHARED JOB PIPELINE =================
const runClipJob = ({ requestId, clips, webhook, minClips, maxClips, sortClips, composerFor, uploadFolder, opts = {} }) => {
  jobLimit(async () => {
    let tempDir;

    try {
      await sendWebhook(webhook, { jobId: requestId, status: "started" });
      updateJob(requestId, { status: "processing" });

      if (!clips || clips.length < minClips) {
        throw new Error(`Need at least ${minClips} clip(s)`);
      }
      if (clips.length > maxClips) throw new Error("Too many clips");

      const parsed = clips.map(parseClipEntry);
      const ordered = sortClips
        ? [...parsed].sort((a, b) => extractTrailingNumber(a.url) - extractTrailingNumber(b.url))
        : parsed;

      tempDir = path.join(TMP_ROOT, requestId);
      fs.mkdirSync(tempDir, { recursive: true });

      // ================= DOWNLOAD =================
      const dlLimit = pLimit(DOWNLOAD_CONCURRENCY);
      const localClips = await Promise.all(
        ordered.map((clip, i) =>
          dlLimit(async () => {
            const file = path.join(tempDir, `clip_${i}.mp4`);
            await downloadFile(clip.url, file);
            return file;
          })
        )
      );

      updateJob(requestId, { step: "normalizing" });

      // ================= NORMALIZE =================
      // ✅ MEMORY FIX: delete each source clip right after it's normalized
      // instead of holding on to every original + every normalized copy
      // simultaneously until the whole job finishes. If TMP_ROOT turns
      // out to be tmpfs (RAM-backed), this materially lowers peak usage;
      // even on real disk it reduces fd/inode pressure during long jobs.
      const normalizedRaw = [];
      for (let i = 0; i < localClips.length; i++) {
        const out = path.join(tempDir, `norm_${i}.mp4`);
        await normalizeClip(localClips[i], out, requestId, webhook);
        fs.rmSync(localClips[i], { force: true });
        normalizedRaw.push(out);
      }

      // ================= PER-CLIP EFFECTS =================
      updateJob(requestId, { step: "applying_effects" });
      const normalized = [];
      for (let i = 0; i < normalizedRaw.length; i++) {
        const out = path.join(tempDir, `fx_${i}.mp4`);
        await applyClipEffects(normalizedRaw[i], out, ordered[i].effects, requestId, webhook);
        fs.rmSync(normalizedRaw[i], { force: true });
        normalized.push(out);
      }

      updateJob(requestId, { step: "composing" });

      // ================= COMPOSE =================
      const { fn: composeFn, usedLayout } = composerFor(normalized.length);
      const composedPath = path.join(tempDir, "composed.mp4");
      await composeFn(normalized, composedPath, requestId, webhook);
      for (const f of normalized) fs.rmSync(f, { force: true });

      // ================= FINAL EFFECTS =================
      updateJob(requestId, { step: "finalizing" });
      const outputPath = path.join(tempDir, "output.mp4");
      await applyFinalEffects(composedPath, outputPath, opts, requestId, webhook);
      fs.rmSync(composedPath, { force: true });

      updateJob(requestId, { step: "uploading" });

      // ================= CLOUDINARY CHUNKED UPLOAD =================
      const upload = await uploadToCloudinary(outputPath, uploadFolder);

      fs.rmSync(tempDir, { recursive: true, force: true });

      const result = {
        jobId: requestId,
        status: "done",
        url: upload.secure_url,
        duration: upload.duration,
        bytes: upload.bytes,
        layout: usedLayout,
        _completedAt: Date.now()
      };

      jobs.set(requestId, { ...jobs.get(requestId), ...result });
      await sendWebhook(webhook, result);

    } catch (err) {
      const failPayload = {
        jobId: requestId,
        status: "failed",
        error: err?.response?.data || err.message,
        _completedAt: Date.now()
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

app.get("/effects", auth, (_, res) => {
  res.json({
    layouts: LAYOUTS,
    transitions: TRANSITIONS,
    motionEffects: Object.keys(MOTION_EFFECTS),
    colorGrades: Object.keys(COLOR_GRADES),
    overlays: Object.keys(OVERLAY_EFFECTS)
  });
});

// ================= MERGE =================
app.post("/merge", auth, (req, res) => {
  const requestId = uuidv4();
  jobs.set(requestId, { id: requestId, status: "queued", progress: 0 });

  const { clips, webhook, transition, transitionDuration, finalColorGrade, finalOverlays, textOverlay } = req.body;

  if (transition && !TRANSITIONS.includes(transition)) {
    jobs.set(requestId, { id: requestId, status: "failed", error: `Unknown transition '${transition}'` });
    return res.status(400).json({ error: "Invalid transition" });
  }

  const opts = { transition, transitionDuration, finalColorGrade, finalOverlays, textOverlay };

  runClipJob({
    requestId,
    clips,
    webhook,
    minClips: 2,
    maxClips: MAX_CLIPS,
    sortClips: true,
    composerFor: (clipCount) => resolveComposer("cut", clipCount, opts),
    uploadFolder: "ai-movies/episodes",
    opts
  });

  res.json({ jobId: requestId, statusUrl: `/status/${requestId}` });
});

// ================= COMPOSE =================
app.post("/compose", auth, (req, res) => {
  const requestId = uuidv4();
  jobs.set(requestId, { id: requestId, status: "queued", progress: 0 });

  const {
    clips, webhook,
    transition, transitionDuration,
    pipCorner, pipScale, pipAudio,
    finalColorGrade, finalOverlays, textOverlay
  } = req.body;
  const layout = (req.body.layout || "cut").toLowerCase();

  if (!LAYOUTS.includes(layout)) {
    jobs.set(requestId, {
      id: requestId,
      status: "failed",
      error: `Unknown layout '${layout}'. Expected one of: ${LAYOUTS.join(", ")}.`
    });
    return res.status(400).json({ error: "Invalid layout" });
  }

  if (transition && !TRANSITIONS.includes(transition)) {
    jobs.set(requestId, { id: requestId, status: "failed", error: `Unknown transition '${transition}'` });
    return res.status(400).json({ error: "Invalid transition" });
  }

  const opts = { transition, transitionDuration, pipCorner, pipScale, pipAudio, finalColorGrade, finalOverlays, textOverlay };

  const minClipsByLayout = { zoom: 1, split: 2, pip: 2, grid4: 4, triptych: 3, overlay: 2, cut: 2 };

  runClipJob({
    requestId,
    clips,
    webhook,
    minClips: minClipsByLayout[layout] ?? 2,
    maxClips: MAX_CLIPS,
    sortClips: true,
    composerFor: (clipCount) => resolveComposer(layout, clipCount, opts),
    uploadFolder: "ai-movies/scenes",
    opts
  });

  res.json({ jobId: requestId, statusUrl: `/status/${requestId}` });
});

// ================= START =================
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT} (MAX_CONCURRENT_JOBS=${MAX_CONCURRENT_JOBS}, FFMPEG_THREADS=${FFMPEG_THREADS}, TMP_ROOT=${TMP_ROOT})`);
});
