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
const MAX_CONCURRENT_FFMPEG = Math.max(1, Number(process.env.MAX_CONCURRENT_FFMPEG || 2));

let activeFfmpegJobs = 0;
const ffmpegWaitQueue = [];

[UPLOAD_DIR, OUTPUT_DIR].forEach(d => fs.mkdirSync(d, { recursive: true }));

app.use(cors());
app.use(express.json());

// ========= JOB STORE =========
const jobs = new Map(); // jobId -> { status, progress, outputPath, error, createdAt }
const JOB_TTL_MS = 30 * 60 * 1000; // 30 min

function createJob() {
  const id = uuidv4();
  jobs.set(id, { status: 'queued', progress: 0, outputPath: null, error: null, createdAt: Date.now() });
  return id;
}

function updateJob(id, updates) {
  const job = jobs.get(id);
  if (job) Object.assign(job, updates);
}

// Cleanup old jobs every 5 min
setInterval(() => {
  const now = Date.now();
  for (const [id, job] of jobs.entries()) {
    if (now - job.createdAt > JOB_TTL_MS) {
      if (job.outputPath) try { fs.unlinkSync(job.outputPath); } catch {}
      jobs.delete(id);
    }
  }
}, 5 * 60 * 1000);

// ========= FFMPEG QUEUE =========
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
    console.log('[FFmpeg] Starting command:', cmd.join(' '));
    const result = await execFileAsync('ffmpeg', cmd, {
      timeout: 300000, // 5 min hard timeout
      maxBuffer: 20 * 1024 * 1024,
    });
    console.log('[FFmpeg] Command completed successfully');
    return result;
  } catch (err) {
    console.error('[FFmpeg] Command failed:', err.message);
    throw err;
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
  res.json({ status: 'ok', ffmpeg: true, activeJobs: activeFfmpegJobs, queuedJobs: ffmpegWaitQueue.length });
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

// ========= ASYNC PROCESS ENDPOINT =========
app.post('/api/process-async', auth, async (req, res) => {
  const { sessionId, videoUrl, config } = req.body;
  const assets = sessionAssets.get(sessionId);

  if (!videoUrl) {
    return res.status(400).json({ error: 'No video URL provided' });
  }

  const jobId = createJob();
  res.json({ jobId, status: 'queued' });

  // Process in background (non-blocking)
  processJobAsync(jobId, videoUrl, config || {}, assets || {}).catch(err => {
    console.error(`Job ${jobId} failed:`, err);
    updateJob(jobId, { status: 'failed', error: getExecErrorDetails(err) });
  });
});

async function processJobAsync(jobId, videoUrl, config, assets) {
  updateJob(jobId, { status: 'downloading', progress: 10 });

  const inputPath = path.join(UPLOAD_DIR, `${uuidv4()}.mp4`);
  const outputPath = path.join(OUTPUT_DIR, `${uuidv4()}.mp4`);

  try {
    // Download video
    const response = await fetch(videoUrl, { signal: AbortSignal.timeout(60000) });
    if (!response.ok) throw new Error(`Download failed: ${response.status}`);
    const buffer = Buffer.from(await response.arrayBuffer());
    fs.writeFileSync(inputPath, buffer);

    updateJob(jobId, { status: 'probing', progress: 30 });

    const probeInfo = await probeVideo(inputPath);
    if (probeInfo.unsupportedCodec) {
      cleanup(inputPath, outputPath);
      updateJob(jobId, { status: 'failed', error: `Codec não suportado: ${probeInfo.codecTag || probeInfo.codecName}` });
      return;
    }

    updateJob(jobId, { status: 'processing', progress: 40 });

    const cmd = buildFFmpegCommand(inputPath, outputPath, config, assets, probeInfo);
    console.log(`Job ${jobId} FFmpeg:`, cmd.join(' '));

    let usedSafeAudioFallback = false;
    try {
      await runWithFfmpegQueue(cmd);
    } catch (primaryErr) {
      console.warn(`Job ${jobId} primary FFmpeg failed:`, getExecErrorDetails(primaryErr));
      usedSafeAudioFallback = true;
      const safeCmd = buildFFmpegCommand(inputPath, outputPath, config, assets, probeInfo, { skipAllAudio: true });
      await runWithFfmpegQueue(safeCmd);
    }

    updateJob(jobId, { progress: 90 });

    const stat = fs.statSync(outputPath);
    if (stat.size < 1024) {
      cleanup(inputPath, outputPath);
      updateJob(jobId, { status: 'failed', error: 'Output file too small' });
      return;
    }

    cleanup(inputPath); // keep outputPath for download
    updateJob(jobId, {
      status: 'done',
      progress: 100,
      outputPath,
      fileSize: stat.size,
      safeAudioFallback: usedSafeAudioFallback,
    });
  } catch (err) {
    cleanup(inputPath, outputPath);
    updateJob(jobId, { status: 'failed', error: getExecErrorDetails(err) });
  }
}

// ========= JOB STATUS & DOWNLOAD =========
app.get('/api/job/:jobId', auth, (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json({
    status: job.status,
    progress: job.progress,
    error: job.error,
    fileSize: job.fileSize || null,
    safeAudioFallback: job.safeAudioFallback || false,
  });
});

app.get('/api/job/:jobId/download', auth, (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  if (job.status !== 'done' || !job.outputPath) {
    return res.status(400).json({ error: 'Job not ready for download' });
  }

  res.setHeader('Content-Type', 'video/mp4');
  res.setHeader('Content-Disposition', 'attachment; filename="output.mp4"');
  const stream = fs.createReadStream(job.outputPath);
  stream.pipe(res);
  stream.on('end', () => {
    // Clean up after download
    try { fs.unlinkSync(job.outputPath); } catch {}
    jobs.delete(req.params.jobId);
  });
  stream.on('error', () => {
    try { fs.unlinkSync(job.outputPath); } catch {}
    jobs.delete(req.params.jobId);
  });
});

// ========= LEGACY SYNC ENDPOINTS (kept for compatibility) =========
app.post('/api/process', auth, upload.single('video'), async (req, res) => {
  const sessionId = req.body.sessionId || req.query.sessionId;
  const assets = sessionAssets.get(sessionId);

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
      return res.status(422).json({ error: 'Unsupported source codec' });
    }

    const cmd = buildFFmpegCommand(inputPath, outputPath, config, assets || {}, probeInfo);
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
    cleanup(inputPath, outputPath);
    res.status(500).json({ error: 'Processing failed', details: getExecErrorDetails(err) });
  }
});

app.post('/api/process-url', auth, async (req, res) => {
  const { sessionId, videoUrl, config } = req.body;
  const assets = sessionAssets.get(sessionId);

  if (!videoUrl) return res.status(400).json({ error: 'No video URL provided' });

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
      return res.status(422).json({ error: 'Unsupported source codec' });
    }

    const cmd = buildFFmpegCommand(inputPath, outputPath, config || {}, assets || {}, probeInfo);
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
    cleanup(inputPath, outputPath);
    res.status(500).json({ error: 'Processing failed', details: getExecErrorDetails(err) });
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

    return { unsupportedCodec, hasAudio: !!audioStream, codecName, codecTag };
  } catch {
    return { unsupportedCodec: false, hasAudio: true, codecName: '', codecTag: '' };
  }
}

function buildFFmpegCommand(inputPath, outputPath, config, assets, probeInfo = {}, options = {}) {
  const hasSourceAudio = probeInfo.hasAudio !== false;
  const skipAllAudio = options.skipAllAudio === true;
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
      // Use -t to limit popup video duration instead of -stream_loop -1 which can hang
      inputs.push('-t', String(popupDuration), '-i', assets.popupMedia);
    } else {
      inputs.push('-i', assets.popupMedia);
    }

    const endCondition = isPopupVideo
      ? `between(t,${appearAt},${appearAt + popupDuration})`
      : `gte(t,${appearAt})`;

    filterParts.push(
      `[${inputIdx}:v]scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2:color=black,format=rgba,colorchannelmixer=aa=${opacityVal}[ovr]`,
      `[0:v][ovr]overlay=0:0:enable='${endCondition}'[vout]`
    );
    videoOut = '[vout]';
    inputIdx++;
  }

  // Audio handling
  if (!skipAllAudio) {
    const sourceAudioMuted = videoVolumeAfterPopup === 0;
    const useSourceAudio = hasSourceAudio && !sourceAudioMuted;
    const hasPopupAudio = !!assets.popupAudio;
    const hasBgMusic = !!assets.bgMusic;
    const needsAudioMix = hasPopupAudio || hasBgMusic || (useSourceAudio && videoVolumeAfterPopup < 100 && assets.popupMedia);

    if (needsAudioMix) {
      const audioLabels = [];

      if (useSourceAudio) {
        if (videoVolumeAfterPopup < 100 && assets.popupMedia) {
          const volAfter = videoVolumeAfterPopup / 100;
          // Volume starts at 1.0 (100%), then changes to volAfter when popup appears
          filterParts.push(`[0:a]volume='if(lt(t,${appearAt}),1.0,${volAfter})':eval=frame[a_orig]`);
        } else {
          filterParts.push(`[0:a]acopy[a_orig]`);
        }
        audioLabels.push('[a_orig]');
      }

      if (hasPopupAudio) {
        const popVol = popupAudioVolume / 100;
        const delayMs = Math.round(appearAt * 1000);
        inputs.push('-i', assets.popupAudio);
        filterParts.push(`[${inputIdx}:a]volume=${popVol},adelay=${delayMs}|${delayMs}[a_pop]`);
        audioLabels.push('[a_pop]');
        inputIdx++;
      }

      if (hasBgMusic) {
        const bgVol = bgMusicVolume / 100;
        inputs.push('-i', assets.bgMusic);
        filterParts.push(`[${inputIdx}:a]volume=${bgVol}[a_bg]`);
        audioLabels.push('[a_bg]');
        inputIdx++;
      }

      if (audioLabels.length === 0) {
        audioOut = null;
      } else if (audioLabels.length > 1) {
        filterParts.push(
          `${audioLabels.join('')}amix=inputs=${audioLabels.length}:duration=first:dropout_transition=2[a_final]`
        );
        audioOut = '[a_final]';
      } else {
        audioOut = audioLabels[0];
      }
    } else if (useSourceAudio) {
      audioOut = '0:a';
    }
  }

  const cmd = ['-hide_banner', '-loglevel', 'warning', '-nostats', ...inputs];

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

  if (audioOut) {
    cmd.push('-c:a', 'aac', '-b:a', '128k');
  }

  cmd.push('-shortest', '-y', outputPath);
  return cmd;
}

// Cleanup old temp files every 30 min
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
  console.log(`🎬 FFmpeg API running on port ${PORT} (max ${MAX_CONCURRENT_FFMPEG} concurrent)`);
});
