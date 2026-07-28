import ffmpeg from "fluent-ffmpeg";
import fs from "fs-extra";
import path from "path";
import { v4 as uuidv4 } from "uuid";

const TEMP_DIR = "./temp";

await fs.ensureDir(TEMP_DIR);

/**
 * Merge multiple video clips into one
 */
export const mergeVideos = (inputFiles, outputFile) => {
  return new Promise((resolve, reject) => {
    const command = ffmpeg();

    inputFiles.forEach(file => command.input(file));

    command
      .on("error", err => reject(err))
      .on("end", () => resolve(outputFile))
      .mergeToFile(outputFile, TEMP_DIR);
  });
};

/**
 * Trim a video clip
 */
export const trimVideo = (input, start, duration) => {
  const output = path.join(TEMP_DIR, `${uuidv4()}.mp4`);

  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .setStartTime(start)
      .setDuration(duration)
      .output(output)
      .on("end", () => resolve(output))
      .on("error", reject)
      .run();
  });
};

/**
 * Create a scene clip (used for your 15s Magic Hour clips)
 */
export const createSceneClip = (input) => {
  const output = path.join(TEMP_DIR, `${uuidv4()}.mp4`);

  return new Promise((resolve, reject) => {
    ffmpeg(input)
      .outputOptions([
        "-c:v libx264",
        "-preset fast",
        "-crf 23",
        "-c:a aac"
      ])
      .output(output)
      .on("end", () => resolve(output))
      .on("error", reject)
      .run();
  });
};
