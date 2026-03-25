FROM node:20-slim

# Install FFmpeg
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json ./
RUN npm install --production

COPY src/ ./src/

# Create temp directories
RUN mkdir -p /app/tmp/uploads /app/tmp/outputs

EXPOSE 3000

CMD ["node", "src/index.js"]
