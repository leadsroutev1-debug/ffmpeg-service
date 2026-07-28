FROM node:18-slim

# Install ffmpeg (clean + smaller image)
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only package files first (better caching)
COPY package*.json ./

# Install production deps only
RUN npm install --omit=dev

# Copy rest of the app
COPY . .

# Expose port (Render uses 3000 by default)
EXPOSE 3000

# Start app
CMD ["node", "src/index.js"]
