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

// ============================================================
// ================= STUDIO EFFECTS CATALOG (NEW) =============
// ============================================================
// Everything in this section is additive and opt-in. If a caller sends the
// exact same payloads as before (plain string arrays, no `effects`/`opts`
// fields), none of this code path executes and output is byte-identical to
// the original pipeline. New callers can opt into per-clip motion/color/
// overlay effects, cross-fade transitions between clips, and new composite
// layouts, all addressed by name so n8n/Director can pick them from a list
// (see GET /effects).

// ---- Transitions (56) -----------------------------------------------
// These map 1:1 to ffmpeg's native `xfade` transition names, so they're
// real, battle-tested GPU/CPU-friendly cross-dissolve/wipe/slide effects,
// not reinvented ones.
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

// ---- Motion / animation effects (18) ---------------------------------
// Applied to a single already-normalized clip. Each entry returns
// { vf, af } -- af is only present for effects that change playback speed
// (audio has to move in lockstep or it drifts out of sync).
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

// ---- Color grades (26) ------------------------------------------------
// A mix of hand-tuned filter chains and ffmpeg's built-in `curves` presets
// (cross_process, vintage, negative, etc. are native curves presets, not
// approximations).
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

// ---- Overlay effects (8) -----------------------------------------------
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

// ---- Composite layouts added to the original 4 (cut/overlay/split/zoom):
//   grid4, triptych, pip  --> 7 total layouts

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

// Combines an optional motion effect + color grade + list of overlay
// effects for ONE clip into a single { vf, af } pair. Unknown names are
// logged and skipped rather than failing the whole job.
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

// Applies a per-clip effects spec to one normalized clip. If the spec is
// empty, just copies the file through untouched (cheap, no re-encode).
const applyClipEffects = async (input, output, spec, jobId, webhook) => {
  const hasEffects = spec && (spec.motion || spec.colorGrade || (spec.overlays && spec.overlays.length));
  if (!hasEffects) {
    fs.copyFileSync(input, output);
    return;
  }

  const duration = await getDuration(input);
  const { vf, af } = buildEffectFilterChain(spec, duration);

  const args = ["-y", "-i", input];
  if (vf) args.push("-vf", vf);
  args.push("-c:v", USE_GPU ? "h264_nvenc" : "libx264", "-preset", "veryfast", "-crf", "23");
  if (af) {
    args.push("-af", af, "-c:a", "aac");
  } else {
    args.push("-c:a", "copy");
  }
  args.push("-movflags", "+faststart", output);

  await runFFmpeg(args, jobId, webhook);
};

// cut with cross-fade transitions: chains xfade (video) + acrossfade
// (audio) across N clips instead of a hard concat. Falls back to plain
// composeCut if no transition is requested.
const composeCutTransition = async (normalized, outputPath, jobId, webhook, transitionName, transitionDuration) => {
  if (normalized.length === 1) {
    fs.copyFileSync(normalized[0], outputPath);
    return;
  }

  const durations = [];
  for (const c of normalized) durations.push(await getDuration(c));

  const td = Math.max(0.1, Math.min(transitionDuration || 0.5, Math.min(...durations) / 2));

  const filterParts = [];
  const audioParts = [];
  let cumulative = durations[0];
  let vLabel = "0:v";
  let aLabel = "0:a";

  for (let i = 1; i < normalized.length; i++) {
    const offset = Math.max(0, cumulative - td);
    const isLast = i === normalized.length - 1;
    const outV = isLast ? "outv" : `v${i}`;
    const outA = isLast ? "outa" : `a${i}`;

    filterParts.push(`[${vLabel}][${i}:v]xfade=transition=${transitionName}:duration=${td.toFixed(3)}:offset=${offset.toFixed(3)}[${outV}]`);
    audioParts.push(`[${aLabel}][${i}:a]acrossfade=d=${td.toFixed(3)}[${outA}]`);

    vLabel = outV;
    aLabel = outA;
    cumulative = offset + durations[i];
  }

  const filter = [...filterParts, ...audioParts].join(";");

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

const composeCutOrTransition = async (normalized, outputPath, jobId, webhook, opts = {}) => {
  if (opts.transition && TRANSITIONS.includes(opts.transition) && normalized.length > 1) {
    return composeCutTransition(normalized, outputPath, jobId, webhook, opts.transition, opts.transitionDuration);
  }
  return composeCut(normalized, outputPath, jobId, webhook);
};

// grid4: exactly 4 clips in a 2x2 grid, audio mixed from all 4.
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
    "-y",
    ...normalized.slice(0, 4).flatMap(c => ["-i", c]),
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

// triptych: exactly 3 clips side by side (160px columns at 480 width).
const composeTriptych = async (normalized, outputPath, jobId, webhook) => {
  const colW = Math.floor(OUTPUT_WIDTH / 3);

  const filter =
    `[0:v]scale=${colW}:${OUTPUT_HEIGHT}[a];` +
    `[1:v]scale=${colW}:${OUTPUT_HEIGHT}[b];` +
    `[2:v]scale=${OUTPUT_WIDTH - colW * 2}:${OUTPUT_HEIGHT}[c];` +
    `[a][b][c]hstack=inputs=3[outv];` +
    `[0:a][1:a][2:a]amix=inputs=3:duration=shortest:dropout_transition=0[outa]`;

  await runFFmpeg([
    "-y",
    ...normalized.slice(0, 3).flatMap(c => ["-i", c]),
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

// pip: base clip full-frame for its whole duration, second clip as a
// picture-in-picture box in a configurable corner, for the whole duration
// (unlike "overlay", which time-slices multiple overlay clips).
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
    "-y",
    "-i", base,
    "-i", pip,
    "-filter_complex", filter,
    "-map", "[outv]",
    "-map", useAmix ? "[outa]" : "0:a",
    "-c:v", USE_GPU ? "h264_nvenc" : "libx264",
    "-preset", "veryfast",
    "-crf", "23",
    "-c:a", "aac",
    "-movflags", "+faststart",
    outputPath
  ], jobId, webhook);
};

// Final, whole-video post-process: global color grade, global overlays,
// and/or a text card. Skipped (cheap copy) if none of those were requested.
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
    "-y",
    "-i", input,
    "-vf", chain,
    "-c:v", USE_GPU ? "h264_nvenc" : "libx264",
    "-preset", "veryfast",
    "-crf", "23",
    "-c:a", "copy",
    "-movflags", "+faststart",
    output
  ], jobId, webhook);
};

// Resolves the requested layout to a compose function, applying sane
// fallbacks (documented on the n8n side too) rather than failing the whole
// scene over a layout/clip-count mismatch. `opts` carries layout-specific
// extras (transition name/duration, pip corner/scale/audio).
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

// Turns a raw clip entry (string OR {url, motion, colorGrade, overlays})
// into a normalized { url, effects } shape. Old-style string arrays keep
// working exactly as before -- effects is just null for them.
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

// ================= SHARED JOB PIPELINE =================
// Both /merge and /compose do the same thing end to end -- queue, download,
// normalize, [optional per-clip effects], run one ffmpeg composition step,
// [optional final effects], upload, report -- so that pipeline lives in one
// place and each route just supplies the ordering rule, the composer, and
// the upload folder.
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

      tempDir = path.join(os.tmpdir(), requestId);
      fs.mkdirSync(tempDir, { recursive: true });

      // ================= DOWNLOAD =================
      const limit = pLimit(DOWNLOAD_CONCURRENCY);
      const localClips = await Promise.all(
        ordered.map((clip, i) =>
          limit(async () => {
            const file = path.join(tempDir, `clip_${i}.mp4`);
            await downloadFile(clip.url, file);
            return file;
          })
        )
      );

      updateJob(requestId, { step: "normalizing" });

      // ================= NORMALIZE =================
      const normalizedRaw = [];
      for (let i = 0; i < localClips.length; i++) {
        const out = path.join(tempDir, `norm_${i}.mp4`);
        await normalizeClip(localClips[i], out, requestId, webhook);
        normalizedRaw.push(out);
      }

      // ================= PER-CLIP EFFECTS (NEW, opt-in) =================
      updateJob(requestId, { step: "applying_effects" });
      const normalized = [];
      for (let i = 0; i < normalizedRaw.length; i++) {
        const out = path.join(tempDir, `fx_${i}.mp4`);
        await applyClipEffects(normalizedRaw[i], out, ordered[i].effects, requestId, webhook);
        normalized.push(out);
      }

      updateJob(requestId, { step: "composing" });

      // ================= COMPOSE =================
      const { fn: composeFn, usedLayout } = composerFor(normalized.length);
      const composedPath = path.join(tempDir, "composed.mp4");
      await composeFn(normalized, composedPath, requestId, webhook);

      // ================= FINAL EFFECTS (NEW, opt-in) =================
      updateJob(requestId, { step: "finalizing" });
      const outputPath = path.join(tempDir, "output.mp4");
      await applyFinalEffects(composedPath, outputPath, opts, requestId, webhook);

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

// Lets n8n/Director introspect the full catalog instead of hardcoding names.
app.get("/effects", auth, (_, res) => {
  res.json({
    layouts: LAYOUTS,
    transitions: TRANSITIONS,
    motionEffects: Object.keys(MOTION_EFFECTS),
    colorGrades: Object.keys(COLOR_GRADES),
    overlays: Object.keys(OVERLAY_EFFECTS)
  });
});

// ================= MERGE (final episode: composed scene clips -> one video) =================
// Body: { clips: (string | {url, motion, colorGrade, overlays})[], webhook?,
//         transition?, transitionDuration?, finalColorGrade?, finalOverlays?, textOverlay? }
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
    sortClips: true, // scene_NNN URLs -- keep numeric-order safety net from v1
    composerFor: (clipCount) => resolveComposer("cut", clipCount, opts),
    uploadFolder: "ai-movies/episodes",
    opts
  });

  res.json({ jobId: requestId, statusUrl: `/status/${requestId}` });
});

// ================= COMPOSE (one scene: shot clips -> one scene clip) =================
// Body:
//   { clips: (string | {url, motion, colorGrade, overlays})[],
//     layout: "cut" | "overlay" | "split" | "zoom" | "grid4" | "triptych" | "pip",
//     webhook?, transition?, transitionDuration?,
//     pipCorner?, pipScale?, pipAudio?,
//     finalColorGrade?, finalOverlays?, textOverlay? }
// Returns { jobId, statusUrl } immediately, same shape as /merge; poll
// /status/:id for { status: "processing" | "done" | "failed", url, layout }.
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
    // shot clips arrive already ordered (shot_01, shot_02, ...) from the
    // n8n side's Cloudinary folder listing; re-sorting is a safety net,
    // same principle as /merge trusting scene_NNN numbering.
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
  console.log(`🚀 Server running on port ${PORT}`);
});
