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

function getExecErrorDetails(err) {
  const message = String(err?.message || err || 'Unknown FFmpeg error');
  const stderr = typeof err?.stderr === 'string' ? err.stderr.trim() : '';
  if (!stderr) return message;
  const tail = stderr.slice(-1200);
  return `${message}\n${tail}`;
}

const auth = (req, res, next) => {
  if (API_KEY && req.headers['x-api-key'] !== API_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
};

const storage = multer.diskStorage({
  destination: UPLOAD_DIR,
  filename: (req, file, cb) => cb(null, `${uuidv4()}${path.extname(file.originalname)}`),
});
const upload = multer({ storage, limits: { fileSize: 500 * 1024 * 1024 } });

app.get('/health', (req, res) => {
  res.json({ status: 'ok', ffmpeg: true });
});

const sessionAssets = new Map();

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
  setTimeout(() => cleanupSession(sessionId), 2 * 60 * 60 * 1000);

  res.json({ sessionId, assets: Object.keys(assets) });
});

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
    const probeInfo = await probeVideo(inputPath);
    if (probeInfo.unsupportedCodec) {
      cleanup(inputPath, outputPath);
      return res.status(422).json({
        error: 'Unsupported source codec',
        details: `Codec não suportado: ${probeInfo.codecTag || probeInfo.codecName || 'desconhecido'}`,
      });
    }

    const cmd = buildFFmpegCommand(inputPath, outputPath, config, assets || {}, probeInfo);
    console.log('FFmpeg command:', cmd.join(' '));

    const shouldRetrySafeAudio = !!assets?.popupMedia && Number(config?.videoVolumeAfterPopup ?? 100) < 100;
    let usedSafeAudioFallback = false;

    try {
      await runWithFfmpegQueue(cmd);
    } catch (primaryErr) {
      if (!shouldRetrySafeAudio) throw primaryErr;
      usedSafeAudioFallback = true;
      const safeCmd = buildFFmpegCommand(
        inputPath,
        outputPath,
        config,
        assets || {},
        probeInfo,
        { disableSourceDuck: true },
      );
      console.warn('Primary FFmpeg failed, retrying safe audio mode.');
      await runWithFfmpegQueue(safeCmd);
    }

    const stat = fs.statSync(outputPath);
    if (stat.size < 1024) throw new Error('Output file too small');

    if (usedSafeAudioFallback) {
      res.setHeader('X-Processing-Mode', 'safe-audio-fallback');
    }

    res.setHeader('Content-Type', 'video/mp4');
    res.setHeader('Content-Disposition', 'attachment; filename="output.mp4"');
    const stream = fs.createReadStream(outputPath);
    stream.pipe(res);
    stream.on('end', () => cleanup(inputPath, outputPath));
    stream.on('error', () => cleanup(inputPath, outputPath));
  } catch (err) {
    const details = getExecErrorDetails(err);
    console.error('FFmpeg error:', details);
    cleanup(inputPath, outputPath);
    res.status(500).json({ error: 'Processing failed', details });
  }
});

app.post('/api/process-url', auth, async (req, res) => {
  const { sessionId, videoUrl, config } = req.body;
  const assets = sessionAssets.get(sessionId);

  if (!videoUrl) {
    return res.status(400).json({ error: 'No video URL provided' });
  }

  const inputPath = path.join(UPLOAD_DIR, `${uuidv4()}.mp4`);
  const outputPath = path.join(OUTPUT_DIR, `${uuidv4()}.mp4`);

  try {
    const response = await fetch(videoUrl, { signal: AbortSignal.timeout(45000) });
    if (!response.ok) throw new Error(`Download failed: ${response.status}`);
    const buffer = Buffer.from(await response.arrayBuffer());
    fs.writeFileSync(inputPath, buffer);

    const probeInfo = await probeVideo(inputPath);
    if (probeInfo.unsupportedCodec) {
      cleanup(inputPath, outputPath);
      return res.status(422).json({
        error: 'Unsupported source codec',
        details: `Codec não suportado: ${probeInfo.codecTag || probeInfo.codecName || 'desconhecido'}`,
      });
    }

    const cmd = buildFFmpegCommand(inputPath, outputPath, config || {}, assets || {}, probeInfo);
    console.log('FFmpeg command:', cmd.join(' '));

    const shouldRetrySafeAudio = !!assets?.popupMedia && Number(config?.videoVolumeAfterPopup ?? 100) < 100;
    let usedSafeAudioFallback = false;

    try {
      await runWithFfmpegQueue(cmd);
    } catch (primaryErr) {
      if (!shouldRetrySafeAudio) throw primaryErr;
      usedSafeAudioFallback = true;
      const safeCmd = buildFFmpegCommand(
        inputPath,
        outputPath,
        config || {},
        assets || {},
        probeInfo,
        { disableSourceDuck: true },
      );
      console.warn('Primary FFmpeg failed, retrying safe audio mode.');
      await runWithFfmpegQueue(safeCmd);
    }

    const stat = fs.statSync(outputPath);
    if (stat.size < 1024) throw new Error('Output file too small');

    if (usedSafeAudioFallback) {
      res.setHeader('X-Processing-Mode', 'safe-audio-fallback');
    }

    res.setHeader('Content-Type', 'video/mp4');
    res.setHeader('Content-Disposition', 'attachment; filename="output.mp4"');
    const stream = fs.createReadStream(outputPath);
    stream.pipe(res);
    stream.on('end', () => cleanup(inputPath, outputPath));
    stream.on('error', () => cleanup(inputPath, outputPath));
  } catch (err) {
    const details = getExecErrorDetails(err);
    console.error('Process URL error:', details);
    cleanup(inputPath, outputPath);
    res.status(500).json({ error: 'Processing failed', details });
  }
});

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

/**
 * Probe video for codec info AND whether it has an audio stream.
 * Returns { unsupportedCodec, hasAudio, codecName, codecTag }
 */
async function probeVideo(filePath) {
  try {
    const { stdout } = await execFileAsync('ffprobe', [
      '-v', 'error',
      '-show_entries', 'stream=codec_type,codec_name,codec_tag_string,codec_long_name',
      '-show_entries', 'format_tags=encoder,compatible_brands',
      '-of', 'json',
      filePath,
    ], { timeout: 15000, maxBuffer: 2 * 1024 * 1024 });

    const parsed = JSON.parse(stdout || '{}');
    const streams = parsed?.streams || [];
    const formatTags = parsed?.format?.tags || {};
    
    const videoStream = streams.find(s => s.codec_type === 'video') || {};
    const audioStream = streams.find(s => s.codec_type === 'audio');
    
    const codecName = String(videoStream.codec_name || '').toLowerCase();
    const codecTag = String(videoStream.codec_tag_string || '').toLowerCase();
    const codecLong = String(videoStream.codec_long_name || '').toLowerCase();
    const raw = JSON.stringify({ ...videoStream, ...formatTags }).toLowerCase();

    const unsupportedCodec =
      codecName === 'none' || codecName === '' ||
      codecName === 'bvc2' || codecName === 'bytevc2' ||
      codecTag === 'bvc2' || codecTag === 'bytevc2' ||
      raw.includes('bvc2') || raw.includes('bytevc2') || raw.includes('bytevc1') ||
      codecLong.includes('bytedance');

    return {
      unsupportedCodec,
      hasAudio: !!audioStream,
      codecName,
      codecTag,
    };
  } catch {
    return { unsupportedCodec: false, hasAudio: true, codecName: '', codecTag: '' };
  }
}

/**
 * Build FFmpeg command. Now aware of whether source has audio.
 * If source has no audio, generates a silent track with anullsrc.
 */
function buildFFmpegCommand(inputPath, outputPath, config, assets, probeInfo = {}, options = {}) {
  const hasSourceAudio = probeInfo.hasAudio !== false;
  const disableSourceDuck = options.disableSourceDuck === true;
  const inputs = ['-i', inputPath];
  const filterParts = [];
  let videoOut = '0:v';
  let audioOut = null;
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
  const applySourceDuck = assets.popupMedia && videoVolumeAfterPopup < 100 && !disableSourceDuck;
  const needsAudioMix = assets.popupAudio || assets.bgMusic || applySourceDuck;

  if (needsAudioMix) {
    const audioLabels = [];

    // Source audio: use real audio or generate silence
    if (hasSourceAudio) {
      if (applySourceDuck) {
        const volAfter = videoVolumeAfterPopup / 100;
        filterParts.push(
          `[0:a]volume=${volAfter}:enable='gte(t,${appearAt})'[a_orig]`
        );
      } else {
        filterParts.push(`[0:a]acopy[a_orig]`);
      }
    } else {
      // No source audio — generate silent track
      // We add anullsrc as an input
      inputs.push('-f', 'lavfi', '-i', `anullsrc=channel_layout=stereo:sample_rate=44100`);
      filterParts.push(`[${inputIdx}:a]acopy[a_orig]`);
      inputIdx++;
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
  } else if (hasSourceAudio) {
    audioOut = '0:a';
  }

  const cmd = ['-hide_banner', '-loglevel', 'error', '-nostats', ...inputs];

  if (filterParts.length > 0) {
    cmd.push('-filter_complex', filterParts.join(';'));
  }

  cmd.push('-map', videoOut);
  if (audioOut) {
    cmd.push('-map', audioOut + (audioOut.startsWith('[') ? '' : '?'));
  }

  if (needsVideoEncode) {
    cmd.push(
      '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '28',
      '-pix_fmt', 'yuv420p', '-movflags', '+faststart'
    );
  } else {
    cmd.push('-c:v', 'copy');
  }

  if (needsAudioMix || !hasSourceAudio) {
    cmd.push('-c:a', 'aac', '-b:a', '128k');
  } else if (audioOut) {
    cmd.push('-c:a', 'copy');
  }

  cmd.push('-shortest', '-y', outputPath);
  return cmd;
}

setInterval(() => {
  const now = Date.now();
  [UPLOAD_DIR, OUTPUT_DIR].forEach(dir => {
    try {
      fs.readdirSync(dir).forEach(file => {
        const filePath = path.join(dir, file);
        const stat = fs.statSync(filePath);
        if (now - stat.mtimeMs > 60 * 60 * 1000) {
          fs.unlinkSync(filePath);
        }
      });
    } catch {}
  });
}, 30 * 60 * 1000);

app.listen(PORT, () => {
  console.log(`🎬 FFmpeg API running on port ${PORT}`);
});
