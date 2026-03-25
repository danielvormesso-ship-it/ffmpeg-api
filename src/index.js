const express = require('express');
const multer = require('multer');
const cors = require('cors');
const { execFile } = require('child_process');
const { promisify } = require('util');
const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const execFileAsync = promisify(execFile);
const app = express();
const PORT = process.env.PORT || 3000;
const API_KEY = process.env.API_KEY || '';
const TMP_DIR = '/app/tmp';
const UPLOAD_DIR = path.join(TMP_DIR, 'uploads');
const OUTPUT_DIR = path.join(TMP_DIR, 'outputs');
const MAX_CONCURRENT_FFMPEG = Math.max(1, Number(process.env.MAX_CONCURRENT_FFMPEG || 1));

let activeFfmpegJobs = 0;
const ffmpegWaitQueue = [];

// Ensure dirs exist
[UPLOAD_DIR, OUTPUT_DIR].forEach(d => fs.mkdirSync(d, { recursive: true }));

app.use(cors());
app.use(express.json());

async function runWithFfmpegQueue(cmd) {
  await new Promise((resolve) => {
    const tryAcquire = () => {
      if (activeFfmpegJobs < MAX_CONCURRENT_FFMPEG) {
        activeFfmpegJobs++;
        resolve();
        return;
      }
      ffmpegWaitQueue.push(tryAcquire);
    };
    tryAcquire();
  });

  try {
    return await execFileAsync('ffmpeg', cmd, {
      timeout: 600000,
      maxBuffer: 20 * 1024 * 1024,
    });
  } finally {
    activeFfmpegJobs = Math.max(0, activeFfmpegJobs - 1);
    const next = ffmpegWaitQueue.shift();
    if (next) next();
  }
}

// Auth middleware
const auth = (req, res, next) => {
  if (API_KEY && req.headers['x-api-key'] !== API_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
};

// Multer for file uploads
const storage = multer.diskStorage({
  destination: UPLOAD_DIR,
  filename: (req, file, cb) => cb(null, `${uuidv4()}${path.extname(file.originalname)}`),
});
const upload = multer({ storage, limits: { fileSize: 500 * 1024 * 1024 } });

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', ffmpeg: true });
});

// Upload assets (popup media, popup audio, bg music) — stored for the session
const sessionAssets = new Map(); // sessionId -> { popupMedia, popupAudio, bgMusic }

app.post('/api/upload-assets', auth, upload.fields([
  { name: 'popupMedia', maxCount: 1 },
  { name: 'popupAudio', maxCount: 1 },
  { name: 'bgMusic', maxCount: 1 },
]), (req, res) => {
  const sessionId = uuidv4();
  const assets = {};

  if (req.files?.popupMedia?.[0]) assets.popupMedia = req.files.popupMedia[0].path;
  if (req.files?.popupAudio?.[0]) assets.popupAudio = req.files.popupAudio[0].path;
  if (req.files?.bgMusic?.[0]) assets.bgMusic = req.files.bgMusic[0].path;

  sessionAssets.set(sessionId, assets);

  // Auto-cleanup after 2 hours
  setTimeout(() => cleanupSession(sessionId), 2 * 60 * 60 * 1000);

  res.json({ sessionId, assets: Object.keys(assets) });
});

// Process a single video
app.post('/api/process', auth, upload.single('video'), async (req, res) => {
  const sessionId = req.body.sessionId || req.query.sessionId;
  const assets = sessionAssets.get(sessionId);

  if (!assets && !req.file) {
    return res.status(400).json({ error: 'No video or session provided' });
  }

  if (!req.file) {
    return res.status(400).json({ error: 'No video file provided' });
  }

  const config = JSON.parse(req.body.config || '{}');
  const inputPath = req.file.path;
  const outputPath = path.join(OUTPUT_DIR, `${uuidv4()}.mp4`);

  try {
    const cmd = buildFFmpegCommand(inputPath, outputPath, config, assets || {});
    console.log('FFmpeg command:', cmd.join(' '));

    await runWithFfmpegQueue(cmd);

    const stat = fs.statSync(outputPath);
    if (stat.size < 1024) {
      throw new Error('Output file too small');
    }

    res.setHeader('Content-Type', 'video/mp4');
    res.setHeader('Content-Disposition', 'attachment; filename="output.mp4"');
    const stream = fs.createReadStream(outputPath);
    stream.pipe(res);
    stream.on('end', () => {
      cleanup(inputPath, outputPath);
    });
    stream.on('error', () => {
      cleanup(inputPath, outputPath);
    });
  } catch (err) {
    console.error('FFmpeg error:', err.message);
    cleanup(inputPath, outputPath);
    res.status(500).json({ error: 'Processing failed', details: err.message });
  }
});

// Process video from URL (no upload needed)
app.post('/api/process-url', auth, async (req, res) => {
  const { sessionId, videoUrl, config } = req.body;
  const assets = sessionAssets.get(sessionId);

  if (!videoUrl) {
    return res.status(400).json({ error: 'No video URL provided' });
  }

  const inputPath = path.join(UPLOAD_DIR, `${uuidv4()}.mp4`);
  const outputPath = path.join(OUTPUT_DIR, `${uuidv4()}.mp4`);

  try {
    // Download video
    const response = await fetch(videoUrl, { signal: AbortSignal.timeout(45000) });
    if (!response.ok) throw new Error(`Download failed: ${response.status}`);
    const buffer = Buffer.from(await response.arrayBuffer());
    fs.writeFileSync(inputPath, buffer);

    const codecInfo = await probeVideoCodec(inputPath);
    if (codecInfo.unsupported) {
      cleanup(inputPath, outputPath);
      return res.status(422).json({
        error: 'Unsupported source codec',
        details: `Codec não suportado: ${codecInfo.codecTag || codecInfo.codecName || 'desconhecido'}`,
      });
    }

    const cmd = buildFFmpegCommand(inputPath, outputPath, config || {}, assets || {});
    console.log('FFmpeg command:', cmd.join(' '));

    await runWithFfmpegQueue(cmd);

    const stat = fs.statSync(outputPath);
    if (stat.size < 1024) throw new Error('Output file too small');

    res.setHeader('Content-Type', 'video/mp4');
    res.setHeader('Content-Disposition', 'attachment; filename="output.mp4"');
    const stream = fs.createReadStream(outputPath);
    stream.pipe(res);
    stream.on('end', () => cleanup(inputPath, outputPath));
    stream.on('error', () => cleanup(inputPath, outputPath));
  } catch (err) {
    console.error('Process URL error:', err.message);
    cleanup(inputPath, outputPath);
    res.status(500).json({ error: 'Processing failed', details: err.message });
  }
});

// Cleanup session
app.delete('/api/session/:sessionId', auth, (req, res) => {
  cleanupSession(req.params.sessionId);
  res.json({ ok: true });
});

function cleanupSession(sessionId) {
  const assets = sessionAssets.get(sessionId);
  if (assets) {
    Object.values(assets).forEach(p => { try { fs.unlinkSync(p); } catch {} });
    sessionAssets.delete(sessionId);
  }
}

function cleanup(...files) {
  files.forEach(f => { try { fs.unlinkSync(f); } catch {} });
}

async function probeVideoCodec(filePath) {
  try {
    const { stdout } = await execFileAsync('ffprobe', [
      '-v', 'error',
      '-select_streams', 'v:0',
      '-show_entries', 'stream=codec_name,codec_tag_string,codec_long_name',
      '-of', 'json',
      filePath,
    ], {
      timeout: 15000,
      maxBuffer: 2 * 1024 * 1024,
    });

    const parsed = JSON.parse(stdout || '{}');
    const stream = parsed?.streams?.[0] || {};
    const codecName = String(stream.codec_name || '').toLowerCase();
    const codecTag = String(stream.codec_tag_string || '').toLowerCase();
    const raw = JSON.stringify(stream).toLowerCase();

    const unsupported =
      codecName === 'none' ||
      codecName === '' ||
      codecName === 'bvc2' ||
      codecName === 'bytevc2' ||
      codecTag === 'bvc2' ||
      codecTag === 'bytevc2' ||
      raw.includes('bvc2') ||
      raw.includes('bytevc2');

    return { unsupported, codecName, codecTag };
  } catch {
    return { unsupported: false, codecName: '', codecTag: '' };
  }
}

function buildFFmpegCommand(inputPath, outputPath, config, assets) {
  const inputs = ['-i', inputPath];
  const filterParts = [];
  let videoOut = '0:v';
  let audioOut = '0:a';
  let inputIdx = 1;
  let needsVideoEncode = false;

  const appearAt = config.appearAt ?? 5;
  const popupDuration = config.popupDuration ?? 10;
  const opacity = config.opacity ?? 100;
  const popupAudioVolume = config.popupAudioVolume ?? 100;
  const videoVolumeAfterPopup = config.videoVolumeAfterPopup ?? 100;
  const bgMusicVolume = config.backgroundMusicVolume ?? 100;
  const endVideoWithPopup = config.endVideoWithPopup ?? true;
  const popupMediaType = config.popupMediaType ?? 'image';
  const totalDuration = endVideoWithPopup ? appearAt + popupDuration : 0;

  if (totalDuration > 0) {
    inputs.unshift('-t', String(totalDuration));
  }

  // Popup media overlay
  if (assets.popupMedia) {
    needsVideoEncode = true;
    const opacityVal = opacity / 100;
    const isPopupVideo = popupMediaType === 'video';

    if (isPopupVideo) {
      inputs.push('-stream_loop', '-1', '-i', assets.popupMedia);
    } else {
      inputs.push('-i', assets.popupMedia);
    }

    const endCondition = isPopupVideo
      ? `between(t,${appearAt},${appearAt + popupDuration})`
      : `gte(t,${appearAt})`;

    filterParts.push(
      `[${inputIdx}:v]scale=1080:1920:force_original_aspect_ratio=disable,format=rgba,colorchannelmixer=aa=${opacityVal}[ovr]`,
      `[0:v][ovr]overlay=0:0:enable='${endCondition}'[vout]`
    );
    videoOut = '[vout]';
    inputIdx++;
  }

  // Audio mixing
  const needsAudioMix = assets.popupAudio || assets.bgMusic || (assets.popupMedia && videoVolumeAfterPopup < 100);

  if (needsAudioMix) {
    const audioLabels = [];

    if (assets.popupMedia && videoVolumeAfterPopup < 100) {
      const volAfter = videoVolumeAfterPopup / 100;
      filterParts.push(
        `[0:a]volume='if(gte(t,${appearAt}),${volAfter},1)':eval=frame[a_orig]`
      );
    } else {
      filterParts.push(`[0:a]acopy[a_orig]`);
    }
    audioLabels.push('[a_orig]');

    if (assets.popupAudio) {
      const popVol = popupAudioVolume / 100;
      const delayMs = Math.round(appearAt * 1000);
      inputs.push('-i', assets.popupAudio);
      filterParts.push(
        `[${inputIdx}:a]volume=${popVol},adelay=${delayMs}|${delayMs}[a_pop]`
      );
      audioLabels.push('[a_pop]');
      inputIdx++;
    }

    if (assets.bgMusic) {
      const bgVol = bgMusicVolume / 100;
      inputs.push('-i', assets.bgMusic);
      filterParts.push(`[${inputIdx}:a]volume=${bgVol}[a_bg]`);
      audioLabels.push('[a_bg]');
      inputIdx++;
    }

    if (audioLabels.length > 1) {
      filterParts.push(
        `${audioLabels.join('')}amix=inputs=${audioLabels.length}:duration=first:dropout_transition=2[a_final]`
      );
      audioOut = '[a_final]';
    } else {
      audioOut = audioLabels[0];
    }
  }

  const cmd = ['-hide_banner', '-loglevel', 'error', '-nostats', ...inputs];

  if (filterParts.length > 0) {
    cmd.push('-filter_complex', filterParts.join(';'));
  }

  cmd.push('-map', videoOut, '-map', audioOut + '?');

  if (needsVideoEncode) {
    cmd.push(
      '-c:v', 'libx264',
      '-preset', 'ultrafast',
      '-crf', '28',
      '-pix_fmt', 'yuv420p',
      '-movflags', '+faststart'
    );
  } else {
    cmd.push('-c:v', 'copy');
  }

  if (needsAudioMix) {
    cmd.push('-c:a', 'aac', '-b:a', '128k'); // Better audio quality on server
  } else {
    cmd.push('-c:a', 'copy');
  }

  cmd.push('-shortest', '-y', outputPath);
  return cmd;
}

// Cleanup stale files every 30 minutes
setInterval(() => {
  const now = Date.now();
  [UPLOAD_DIR, OUTPUT_DIR].forEach(dir => {
    try {
      fs.readdirSync(dir).forEach(file => {
        const filePath = path.join(dir, file);
        const stat = fs.statSync(filePath);
        if (now - stat.mtimeMs > 60 * 60 * 1000) { // 1 hour old
          fs.unlinkSync(filePath);
        }
      });
    } catch {}
  });
}, 30 * 60 * 1000);

app.listen(PORT, () => {
  console.log(`🎬 FFmpeg API running on port ${PORT}`);
});
