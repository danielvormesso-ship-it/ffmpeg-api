const express = require('express');
const multer = require('multer');
const cors = require('cors');
const { execFile, spawn } = require('child_process');
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
const MAX_CONCURRENT_FFMPEG = Math.max(1, Number(process.env.MAX_CONCURRENT_FFMPEG || 4));

let activeFfmpegJobs = 0;
const ffmpegWaitQueue = [];
const ffmpegRuntime = {
  checked: false,
  hasLibx264: null,
  encoderListTail: '',
};

[UPLOAD_DIR, OUTPUT_DIR].forEach(d => fs.mkdirSync(d, { recursive: true }));

app.use(cors());
app.use(express.json());

// ========= JOB STORE =========
const jobs = new Map(); // jobId -> { status, progress, outputPath, error, createdAt }
const JOB_TTL_MS = 30 * 60 * 1000; // 30 min

function createJob() {
  const id = uuidv4();
  jobs.set(id, {
    status: 'queued',
    progress: 0,
    outputPath: null,
    error: null,
    createdAt: Date.now(),
    safeAudioFallback: false,
    fallbackMode: 'none',
    attemptErrors: [],
  });
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
async function detectFfmpegRuntime() {
  if (ffmpegRuntime.checked) return ffmpegRuntime;
  try {
    const { stdout } = await execFileAsync('ffmpeg', ['-hide_banner', '-encoders'], {
      timeout: 20000,
      maxBuffer: 4 * 1024 * 1024,
    });
    const encoders = String(stdout || '').toLowerCase();
    ffmpegRuntime.hasLibx264 = encoders.includes('libx264');
    ffmpegRuntime.encoderListTail = encoders.slice(-1200);
  } catch (err) {
    ffmpegRuntime.hasLibx264 = null;
    ffmpegRuntime.encoderListTail = getExecErrorDetails(err);
  } finally {
    ffmpegRuntime.checked = true;
  }
  return ffmpegRuntime;
}

function isLibx264MissingError(details) {
  const msg = String(details || '').toLowerCase();
  return msg.includes('unknown encoder') && msg.includes('libx264');
}

function buildMpeg4FallbackCommand(cmd) {
  const out = [];
  for (let i = 0; i < cmd.length; i++) {
    const token = cmd[i];

    if (token === '-preset' || token === '-crf') {
      i += 1;
      continue;
    }

    if (token === 'libx264') {
      out.push('mpeg4');
      continue;
    }

    out.push(token);
  }

  const hasMpeg4 = out.includes('mpeg4');
  const hasQv = out.includes('-q:v');
  if (hasMpeg4 && !hasQv) {
    const yIndex = out.lastIndexOf('-y');
    const insertAt = yIndex >= 0 ? yIndex : out.length;
    out.splice(insertAt, 0, '-q:v', '5');
  }

  return out;
}

function quoteForShellLog(arg) {
  const value = String(arg ?? '');
  if (value.length === 0) return "''";
  if (/^[A-Za-z0-9_/:=+,.@%-]+$/.test(value)) return value;
  return `'${value.replace(/'/g, `'"'"'`)}'`;
}

function formatCommandForLog(binary, args) {
  return [binary, ...args.map(quoteForShellLog)].join(' ');
}

function runFfmpegProcess(args, timeoutMs = 900000) {
  return new Promise((resolve, reject) => {
    const child = spawn('ffmpeg', args, { shell: false });
    let stdout = '';
    let stderr = '';

    const timeout = setTimeout(() => {
      try { child.kill('SIGKILL'); } catch {}
    }, timeoutMs);

    child.stdout.on('data', (data) => {
      stdout += data.toString();
    });

    child.stderr.on('data', (data) => {
      stderr += data.toString();
    });

    child.on('error', (error) => {
      clearTimeout(timeout);
      const err = new Error(`Failed to spawn ffmpeg: ${error.message}`);
      err.stdout = stdout;
      err.stderr = stderr;
      reject(err);
    });

    child.on('close', (code, signal) => {
      clearTimeout(timeout);
      if (code === 0) {
        resolve({ stdout, stderr });
        return;
      }
      const isSigkill = signal === 'SIGKILL' || signal === 'SIGTERM';
      const reason = isSigkill
        ? `Process killed by ${signal} (likely OOM / resource limit exceeded)`
        : `Exit code ${code}`;
      const err = new Error(`FFmpeg failed: ${reason}`);
      err.code = code;
      err.signal = signal;
      err.stdout = stdout;
      err.stderr = stderr;
      err.isOOM = isSigkill;
      reject(err);
    });
  });
}

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
    console.log('[FFmpeg] Starting command:', formatCommandForLog('ffmpeg', cmd));
    const result = await runFfmpegProcess(cmd, 900000);
    console.log('[FFmpeg] Command completed successfully');
    return result;
  } catch (err) {
    const details = getExecErrorDetails(err);
    const canFallbackEncoder = cmd.includes('libx264') && isLibx264MissingError(details);

    if (canFallbackEncoder) {
      const fallbackCmd = buildMpeg4FallbackCommand(cmd);
      console.warn('[FFmpeg] libx264 indisponível. Tentando fallback mpeg4...');
      console.warn('[FFmpeg] Fallback command:', formatCommandForLog('ffmpeg', fallbackCmd));
      const fallbackResult = await runFfmpegProcess(fallbackCmd, 900000);
      return fallbackResult;
    }

    console.error('[FFmpeg] Command failed:', details);
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
  const stdout = typeof err?.stdout === 'string' ? err.stdout.trim() : '';
  const merged = [stderr, stdout].filter(Boolean).join('\n');
  if (!merged) return message;
  const tail = merged.slice(-2200);
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
  res.json({
    status: 'ok',
    ffmpeg: true,
    activeJobs: activeFfmpegJobs,
    queuedJobs: ffmpegWaitQueue.length,
    hasLibx264: ffmpegRuntime.hasLibx264,
  });
});

const sessionAssets = new Map();
const normalizedPopupCache = new Map();

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

// ========= PROBE CODEC ENDPOINT =========
app.post('/api/probe-codec', auth, async (req, res) => {
  const { videoUrl } = req.body;
  if (!videoUrl) return res.status(400).json({ error: 'No videoUrl provided' });

  const tmpPath = path.join(UPLOAD_DIR, `${uuidv4()}_probe.mp4`);
  try {
    // Download only first 2MB for fast probing
    const controller = new AbortController();
    const response = await fetch(videoUrl, { signal: controller.signal, headers: { Range: 'bytes=0-2097151' } });
    if (!response.ok && response.status !== 206) throw new Error(`Download failed: ${response.status}`);
    const buffer = Buffer.from(await response.arrayBuffer());
    fs.writeFileSync(tmpPath, buffer);

    const probeInfo = await probeVideo(tmpPath);
    res.json({
      compatible: !probeInfo.unsupportedCodec,
      codecName: probeInfo.codecName,
      codecTag: probeInfo.codecTag,
      width: probeInfo.width,
      height: probeInfo.height,
      hasAudio: probeInfo.hasAudio,
    });
  } catch (err) {
    console.error('probe-codec error:', err);
    // If probe fails, assume compatible to not block the pipeline
    res.json({ compatible: true, codecName: 'unknown', codecTag: 'unknown', error: String(err.message || err).slice(0, 200) });
  } finally {
    try { fs.unlinkSync(tmpPath); } catch {}
  }
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

    ensureFileLooksValid(inputPath, 'vídeo principal', 20 * 1024);

    updateJob(jobId, { status: 'probing', progress: 30 });

    const probeInfo = await probeVideo(inputPath);
    console.log(`Job ${jobId} probe: codec=${probeInfo.codecName} tag=${probeInfo.codecTag} ${probeInfo.width}x${probeInfo.height} audio=${probeInfo.hasAudio}`);

    if (probeInfo.unsupportedCodec) {
      // Try to transcode bvc2/bytevc2 to h264 before giving up
      const transcodePath = path.join(UPLOAD_DIR, `${uuidv4()}_transcoded.mp4`);
      console.log(`Job ${jobId} unsupported codec ${probeInfo.codecTag || probeInfo.codecName}, attempting transcode to h264...`);
      try {
        await runWithFfmpegQueue([
          '-hide_banner', '-loglevel', 'error', '-nostats', '-threads', '2',
          '-i', inputPath,
          '-map', '0:v:0', '-map', '0:a?',
      '-c:v', 'libx264', '-preset', 'medium', '-crf', '22', '-pix_fmt', 'yuv420p',
          '-c:a', 'aac', '-b:a', '192k',
          '-movflags', '+faststart', '-y', transcodePath,
        ]);
        ensureFileLooksValid(transcodePath, 'transcoded video', 20 * 1024);
        // Replace input with transcoded version
        fs.unlinkSync(inputPath);
        fs.renameSync(transcodePath, inputPath);
        // Re-probe to get correct info
        const newProbe = await probeVideo(inputPath);
        Object.assign(probeInfo, newProbe, { unsupportedCodec: false });
        console.log(`Job ${jobId} transcode succeeded: codec=${probeInfo.codecName}`);
      } catch (transcodeErr) {
        try { fs.unlinkSync(transcodePath); } catch {}
        cleanup(inputPath, outputPath);
        const detail = getExecErrorDetails(transcodeErr);
        console.error(`Job ${jobId} transcode failed:`, detail);
        updateJob(jobId, {
          status: 'failed',
          error: `O vídeo usa codec ${probeInfo.codecTag || probeInfo.codecName} que este servidor não consegue converter. Exporte em MP4 H.264 e tente novamente.`,
        });
        return;
      }
    }

    updateJob(jobId, { status: 'processing', progress: 40 });

    let sanitizedAssets = sanitizeAssets(assets);
    const attemptErrors = [];
    let forceSimpleVideoOverlay = false;
    const isVideoPopupRequested = Boolean(sanitizedAssets.popupMedia && config?.popupMediaType === 'video');

    if (isVideoPopupRequested) {
      updateJob(jobId, { status: 'processing', progress: 35 });
      try {
        ensureFileLooksValid(sanitizedAssets.popupMedia, 'popup em vídeo', 8 * 1024);
        sanitizedAssets.popupMedia = await normalizePopupVideoAsset(sanitizedAssets.popupMedia);
      } catch (normalizeErr) {
        const normalizeDetails = getExecErrorDetails(normalizeErr);
        attemptErrors.push(`normalize: ${normalizeDetails}`);
        forceSimpleVideoOverlay = true;
        console.warn(`Job ${jobId} popup normalize failed, using simplified overlay path:`, normalizeDetails);
      }
    }

    const cmd = buildFFmpegCommand(inputPath, outputPath, config, sanitizedAssets, probeInfo, {
      simpleVideoOverlay: forceSimpleVideoOverlay,
    });
    console.log(`Job ${jobId} FFmpeg:`, cmd.join(' '));

    let usedSafeAudioFallback = false;
    let fallbackMode = 'none';

    try {
      await runWithFfmpegQueue(cmd);
    } catch (primaryErr) {
      const primaryDetails = getExecErrorDetails(primaryErr);
      attemptErrors.push(`primary: ${primaryDetails}`);
      console.warn(`Job ${jobId} primary FFmpeg failed${primaryErr.isOOM ? ' (OOM/SIGKILL)' : ''}:`, primaryDetails);
      // Clean broken output before retry
      try { fs.unlinkSync(outputPath); } catch {}
      usedSafeAudioFallback = true;
      fallbackMode = 'audio_simplified';

      try {
        const resilientCmd = buildFFmpegCommand(inputPath, outputPath, config, sanitizedAssets, probeInfo, {
          sourceAudioOnly: true,
          simplifiedOverlay: true,
          simpleVideoOverlay: true,
        });
        console.log(`Job ${jobId} FFmpeg resilient fallback:`, resilientCmd.join(' '));
        await runWithFfmpegQueue(resilientCmd);
      } catch (resilientErr) {
        const resilientDetails = getExecErrorDetails(resilientErr);
        attemptErrors.push(`resilient: ${resilientDetails}`);
        console.warn(`Job ${jobId} resilient fallback failed${resilientErr.isOOM ? ' (OOM/SIGKILL)' : ''}:`, resilientDetails);
        // Clean broken output before retry
        try { fs.unlinkSync(outputPath); } catch {}

        const effectsRequested = Boolean(sanitizedAssets.popupMedia || sanitizedAssets.popupAudio);
        if (effectsRequested) {
          throw new Error(
            `Falha ao manter popup/áudio no servidor. Tentativas: ${attemptErrors.join(' | ').slice(0, 3000)}`
          );
        }

        // Last resort only when no popup/effect is requested
        const noPopupCmd = buildFFmpegCommand(inputPath, outputPath, config, sanitizedAssets, probeInfo, {
          skipAllAudio: true,
          noPopupMedia: true,
        });
        console.log(`Job ${jobId} FFmpeg no-popup fallback:`, noPopupCmd.join(' '));
        await runWithFfmpegQueue(noPopupCmd);
        fallbackMode = 'no_popup';
      }
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
      fallbackMode,
      attemptErrors,
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
    fallbackMode: job.fallbackMode || 'none',
    attemptErrors: Array.isArray(job.attemptErrors) ? job.attemptErrors : [],
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
    const popupOriginal = assets.popupMedia;

    Object.values(assets).forEach(p => { try { fs.unlinkSync(p); } catch {} });

    if (popupOriginal && normalizedPopupCache.has(popupOriginal)) {
      const normalizedPath = normalizedPopupCache.get(popupOriginal);
      if (normalizedPath) {
        try { fs.unlinkSync(normalizedPath); } catch {}
      }
      normalizedPopupCache.delete(popupOriginal);
    }

    sessionAssets.delete(sessionId);
  }
}

function cleanup(...files) {
  files.forEach(f => { try { fs.unlinkSync(f); } catch {} });
}

function sanitizeAssetPath(filePath) {
  if (!filePath || typeof filePath !== 'string') return null;
  if (filePath === '/' || filePath.length < 5) return null;

  const resolved = path.resolve(filePath);
  if (!resolved.startsWith(UPLOAD_DIR + path.sep)) return null;

  try {
    const stat = fs.statSync(resolved);
    if (!stat.isFile()) return null;
    return resolved;
  } catch {
    return null;
  }
}

function sanitizeAssets(assets = {}) {
  return {
    popupMedia: sanitizeAssetPath(assets.popupMedia),
    popupAudio: sanitizeAssetPath(assets.popupAudio),
    bgMusic: sanitizeAssetPath(assets.bgMusic),
  };
}

function ensureFileLooksValid(filePath, label, minBytes = 1024) {
  if (!filePath || !fs.existsSync(filePath)) {
    throw new Error(`${label} ausente: ${filePath || 'caminho vazio'}`);
  }

  const stat = fs.statSync(filePath);
  if (!stat.isFile()) {
    throw new Error(`${label} inválido: não é arquivo`);
  }

  if (stat.size < minBytes) {
    throw new Error(`${label} muito pequeno (${stat.size} bytes)`);
  }

  return stat.size;
}

async function normalizePopupVideoAsset(filePath) {
  const cached = normalizedPopupCache.get(filePath);
  if (cached && fs.existsSync(cached)) {
    return cached;
  }

  const normalizedPath = path.join(UPLOAD_DIR, `${uuidv4()}_popup_normalized.mp4`);
  const attemptErrors = [];

  // ---- Attempt 1: Stream copy (zero CPU/memory cost) ----
  const copyCmd = [
    '-hide_banner', '-loglevel', 'error', '-nostats', '-threads', '1',
    '-i', filePath,
    '-an',
    '-c:v', 'copy',
    '-movflags', '+faststart',
    '-y', normalizedPath,
  ];

  try {
    console.log('[Normalize] Attempt 1: stream copy (no re-encode)');
    await runWithFfmpegQueue(copyCmd);
    // Validate output
    if (fs.existsSync(normalizedPath) && fs.statSync(normalizedPath).size > 1024) {
      normalizedPopupCache.set(filePath, normalizedPath);
      console.log('[Normalize] Stream copy succeeded');
      return normalizedPath;
    }
    throw new Error('Output too small after stream copy');
  } catch (errA) {
    attemptErrors.push(`stream_copy: ${getExecErrorDetails(errA)}`);
    console.warn('[Normalize] Stream copy failed, trying lightweight re-encode...');
    // Clean failed output
    try { fs.unlinkSync(normalizedPath); } catch {}
  }

  // ---- Attempt 2: Lightweight re-encode at 720x1280 (saves ~75% memory vs 1080x1920) ----
  const lightCmd = [
    '-hide_banner', '-loglevel', 'error', '-nostats',
    '-i', filePath,
    '-an',
    '-threads', '2',
    '-vf', 'scale=720:1280:force_original_aspect_ratio=increase,crop=720:1280,setsar=1,format=yuv420p',
    '-c:v', 'libx264', '-preset', 'medium', '-crf', '22',
    '-pix_fmt', 'yuv420p', '-movflags', '+faststart',
    '-y', normalizedPath,
  ];

  try {
    console.log('[Normalize] Attempt 2: lightweight re-encode at 720x1280');
    await runWithFfmpegQueue(lightCmd);
    if (fs.existsSync(normalizedPath) && fs.statSync(normalizedPath).size > 1024) {
      normalizedPopupCache.set(filePath, normalizedPath);
      console.log('[Normalize] Lightweight re-encode succeeded');
      return normalizedPath;
    }
    throw new Error('Output too small after lightweight re-encode');
  } catch (errB) {
    attemptErrors.push(`light_reencode: ${getExecErrorDetails(errB)}`);
    try { fs.unlinkSync(normalizedPath); } catch {}

    // If both attempts were killed (OOM), throw specific error
    if (errB.isOOM) {
      throw new Error(`Popup normalization killed by OS (OOM). Attempts: ${attemptErrors.join(' | ').slice(0, 2000)}`);
    }
    throw new Error(`Popup normalization failed. Attempts: ${attemptErrors.join(' | ').slice(0, 2000)}`);
  }
}

async function probeVideo(filePath) {
  try {
    const { stdout } = await execFileAsync('ffprobe', [
      '-v', 'error',
      '-show_entries', 'stream=codec_type,codec_name,codec_tag_string,codec_long_name,width,height',
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

    // Get resolution (with even-number rounding for codec compatibility)
    const w = videoStream.width ? Math.round(videoStream.width / 2) * 2 : 1080;
    const h = videoStream.height ? Math.round(videoStream.height / 2) * 2 : 1920;

    return { unsupportedCodec, hasAudio: !!audioStream, codecName, codecTag, width: w, height: h };
  } catch {
    return { unsupportedCodec: false, hasAudio: true, codecName: '', codecTag: '', width: 1080, height: 1920 };
  }
}

function buildFFmpegCommand(inputPath, outputPath, config, assets, probeInfo = {}, options = {}) {
  const hasSourceAudio = probeInfo.hasAudio !== false;
  const skipAllAudio = options.skipAllAudio === true;
  const sourceAudioOnly = options.sourceAudioOnly === true;
  const simplifiedOverlay = options.simplifiedOverlay === true;
  const simpleVideoOverlay = options.simpleVideoOverlay === true;
  const noPopupMedia = options.noPopupMedia === true;
  // Force 720x1280 to prevent OOM on small Railway containers
  const baseW = 720;
  const baseH = 1280;
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
  const popupFullscreen = config.popupFullscreen !== false;
  const popupTransform = config.popupTransform || null; // { x, y, width, height, rotation } in percentages
  const hasCustomTransform = Boolean(
    popupTransform && (
      Math.abs((popupTransform.x ?? 0)) > 0.001 ||
      Math.abs((popupTransform.y ?? 0)) > 0.001 ||
      Math.abs((popupTransform.width ?? 100) - 100) > 0.001 ||
      Math.abs((popupTransform.height ?? 100) - 100) > 0.001 ||
      Math.abs((popupTransform.rotation ?? 0)) > 0.001
    )
  );
  const totalDuration = endVideoWithPopup ? appearAt + popupDuration : 0;
  const usePopupMedia = !!assets.popupMedia && !noPopupMedia;

  if (totalDuration > 0) {
    inputs.unshift('-t', String(totalDuration));
  }

  // Popup media overlay
  if (usePopupMedia) {
    needsVideoEncode = true;
    const opacityVal = opacity / 100;
    const isPopupVideo = popupMediaType === 'video';

    if (isPopupVideo) {
      // -an ignores popup audio (avoids stream conflicts), -itsoffset delays start
      inputs.push('-itsoffset', String(appearAt), '-t', String(popupDuration), '-an', '-i', assets.popupMedia);
    } else {
      inputs.push('-i', assets.popupMedia);
    }

    const overlayInputLabel = `[${inputIdx}:v]`;
    const transformX = Math.max(0, Math.min(100, Number(popupTransform?.x ?? 0)));
    const transformY = Math.max(0, Math.min(100, Number(popupTransform?.y ?? 0)));
    const transformW = Math.max(5, Math.min(100, Number(popupTransform?.width ?? 100)));
    const transformH = Math.max(5, Math.min(100, Number(popupTransform?.height ?? 100)));
    const transformRot = Number(popupTransform?.rotation ?? 0);
    const scaleW = Math.max(2, Math.round((baseW * transformW / 100) / 2) * 2);
    const scaleH = Math.max(2, Math.round((baseH * transformH / 100) / 2) * 2);
    const posX = Math.round(baseW * transformX / 100);
    const posY = Math.round(baseH * transformY / 100);
    const rotateFilter = transformRot !== 0 ? `,rotate=${(transformRot * Math.PI / 180).toFixed(4)}:fillcolor=none` : '';

    if (isPopupVideo) {
      if (hasCustomTransform) {
        filterParts.push(
          `[0:v]scale=${baseW}:${baseH}:force_original_aspect_ratio=increase,crop=${baseW}:${baseH},setsar=1,format=yuv420p[base]`,
          `${overlayInputLabel}scale=${scaleW}:${scaleH},fps=30,setsar=1,format=yuv420p${rotateFilter}[ovr]`,
          `[base][ovr]overlay=${posX}:${posY}:eof_action=pass[vout]`
        );
      } else if (popupFullscreen) {
        filterParts.push(
          `[0:v]scale=${baseW}:${baseH}:force_original_aspect_ratio=increase,crop=${baseW}:${baseH},setsar=1,format=yuv420p[base]`,
          `${overlayInputLabel}scale=${baseW}:${baseH}:force_original_aspect_ratio=increase,crop=${baseW}:${baseH},setsar=1,format=yuv420p[ovr]`,
          `[base][ovr]overlay=0:0:eof_action=pass[vout]`
        );
      } else {
        filterParts.push(
          `[0:v]scale=${baseW}:${baseH}:force_original_aspect_ratio=increase,crop=${baseW}:${baseH},setsar=1,format=yuv420p[base]`,
          `${overlayInputLabel}fps=30,setsar=1,format=yuv420p[ovr]`,
          `[base][ovr]overlay=(W-w)/2:(H-h)/2:eof_action=pass[vout]`
        );
      }
    } else {
      const endAt = appearAt + popupDuration;

      if (hasCustomTransform) {
        const overlayBaseFilter = simplifiedOverlay
          ? `${overlayInputLabel}scale=${scaleW}:${scaleH},setsar=1,format=yuv420p${rotateFilter}[ovr]`
          : `${overlayInputLabel}scale=${scaleW}:${scaleH},format=rgba,colorchannelmixer=aa=${opacityVal}${rotateFilter}[ovr]`;
        filterParts.push(
          `[0:v]scale=${baseW}:${baseH}:force_original_aspect_ratio=increase,crop=${baseW}:${baseH},setsar=1,format=yuv420p[base]`,
          overlayBaseFilter,
          `[base][ovr]overlay=${posX}:${posY}:enable=between(t\\,${appearAt}\\,${endAt})[vout]`
        );
      } else if (popupFullscreen) {
        const overlayBaseFilter = simplifiedOverlay
          ? `${overlayInputLabel}scale=${baseW}:${baseH}:force_original_aspect_ratio=increase,crop=${baseW}:${baseH},setsar=1,format=yuv420p[ovr]`
          : `${overlayInputLabel}scale=${baseW}:${baseH}:force_original_aspect_ratio=increase,crop=${baseW}:${baseH},format=rgba,colorchannelmixer=aa=${opacityVal}[ovr]`;
        filterParts.push(
          `[0:v]scale=${baseW}:${baseH}:force_original_aspect_ratio=increase,crop=${baseW}:${baseH},setsar=1,format=yuv420p[base]`,
          overlayBaseFilter,
          `[base][ovr]overlay=(W-w)/2:(H-h)/2:enable=between(t\\,${appearAt}\\,${endAt})[vout]`
        );
      } else {
        const overlayBaseFilter = simplifiedOverlay
          ? `${overlayInputLabel}setsar=1,format=yuv420p[ovr]`
          : `${overlayInputLabel}format=rgba,colorchannelmixer=aa=${opacityVal}[ovr]`;
        filterParts.push(
          `[0:v]scale=${baseW}:${baseH}:force_original_aspect_ratio=increase,crop=${baseW}:${baseH},setsar=1,format=yuv420p[base]`,
          overlayBaseFilter,
          `[base][ovr]overlay=(W-w)/2:(H-h)/2:enable=between(t\\,${appearAt}\\,${endAt})[vout]`
        );
      }
    }

    videoOut = '[vout]';
    inputIdx++;
  }

  // Audio handling
  if (!skipAllAudio) {
    const useSourceAudio = hasSourceAudio;

    if (sourceAudioOnly) {
      audioOut = useSourceAudio ? '0:a' : null;
    } else {
      const hasPopupAudio = !!assets.popupAudio;
      const hasBgMusic = !!assets.bgMusic;
      const shouldDuckSource = useSourceAudio && videoVolumeAfterPopup < 100 && usePopupMedia;
      const needsAudioMix = hasPopupAudio || hasBgMusic || shouldDuckSource;

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
  }

  const cmd = ['-hide_banner', '-loglevel', 'error', '-nostats', '-threads', '2', ...inputs];

  if (filterParts.length > 0) {
    cmd.push('-filter_complex', filterParts.join(';'));
  }

  cmd.push('-map', videoOut);
  if (audioOut) {
    cmd.push('-map', audioOut + (audioOut.startsWith('[') ? '' : '?'));
  }

  if (needsVideoEncode) {
    cmd.push(
    '-c:v', 'libx264', '-preset', 'medium', '-crf', '22',
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

  detectFfmpegRuntime()
    .then((info) => {
      console.log(`[FFmpeg] libx264 disponível: ${info.hasLibx264 === true ? 'sim' : info.hasLibx264 === false ? 'não' : 'desconhecido'}`);
    })
    .catch((err) => {
      console.warn('[FFmpeg] Falha ao detectar encoders na inicialização:', getExecErrorDetails(err));
    });
});
