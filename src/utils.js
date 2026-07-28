import { v2 as cloudinary } from "cloudinary";
import fs from "fs-extra";

cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET
});

/**
 * Upload video to Cloudinary
 */
export const uploadToCloudinary = async (filePath) => {
  try {
    const result = await cloudinary.uploader.upload(filePath, {
      resource_type: "video"
    });

    return result.secure_url;
  } catch (error) {
    throw error;
  }
};

/**
 * Delete temp files
 */
export const cleanupFiles = async (files) => {
  for (const file of files) {
    try {
      await fs.remove(file);
    } catch (err) {
      console.error("Cleanup error:", err);
    }
  }
};
