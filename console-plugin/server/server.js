'use strict';

const express = require('express');
const { createLogger } = require('./logger');
const { nanoid } = require('nanoid');
const fetch = require('node-fetch');
const path = require('path');
const fs = require('fs');

const {
  createUploadPod, waitForPodReady, cleanupUploadPod,
  streamChunkToUploadPod, triggerAssembly, createPvc, deletePvc,
} = require('./uploadPod');
const { listPvcs } = require('./collectionPipeline');
const { extractImageSetConfigFromTar } = require('./tarImageSetExtractor');
const { generateCollectionDownloadUrls, resolveCollectionArtifact, streamS3ParallelDownload } = require('./s3Client');

const logger = createLogger('plugin-backend');
const app = express();
const PORT = parseInt(process.env.PORT || '4001', 10);

// ─── Proxy target for shared architect routes ────────────────────────────────
const OAA_BACKEND_URL = process.env.OAA_BACKEND_URL || 'http://localhost:4000';

// ─── In-memory job + upload state ───────────────────────────────────────────
const jobs = new Map();
const activeUploads = new Map();
const activeChunkedUploads = new Map();

function createJob(type, message) {
  const id = nanoid();
  jobs.set(id, { id, type, status: 'running', progress: 0, message, output: '', metadata: {}, createdAt: Date.now() });
  return id;
}
function updateJob(id, patch) { const j = jobs.get(id); if (j) Object.assign(j, patch); }
function appendJobOutput(id, text) { const j = jobs.get(id); if (j) j.output += text; }
function updateJobMetadata(id, patch) { const j = jobs.get(id); if (j) Object.assign(j.metadata, patch); }

// ─── Helpers ─────────────────────────────────────────────────────────────────
function isOperatorManaged() { return process.env.OPENSHIFT_OPERATOR_MANAGED === 'true'; }
function getDeploymentSide() { return process.env.DEPLOYMENT_SIDE || 'connected'; }
function isDisconnected() { return getDeploymentSide() === 'airgapped'; }

const dataDir = process.env.DATA_DIR || '/data';
const CHUNK_SIZE_DEFAULT = 40 * 1024 * 1024;

app.use(express.json());

// ─── Shared routes — proxy to airgap architect backend ──────────────────────
const SHARED_ROUTES = [
  'GET /api/cincinnati/channels',
  'GET /api/cincinnati/patches',
  'GET /api/jobs/:id',
  'POST /api/state',
  'GET /api/state',
  'POST /api/operators/scan',
  'GET /api/operators/status',
  'POST /api/generate',
];

for (const route of SHARED_ROUTES) {
  const [method, routePath] = route.split(' ');
  app[method.toLowerCase()](routePath, async (req, res) => {
    try {
      const qs = Object.keys(req.query).length ? '?' + new URLSearchParams(req.query).toString() : '';
      const upstream = await fetch(`${OAA_BACKEND_URL}${req.path}${qs}`, {
        method: req.method,
        headers: { 'Content-Type': 'application/json' },
        body: ['GET', 'HEAD'].includes(req.method) ? undefined : JSON.stringify(req.body),
      });
      const data = await upstream.json().catch(() => ({}));
      res.status(upstream.status).json(data);
    } catch (err) {
      logger.error({ route: req.path, error: err.message }, 'Upstream proxy error');
      res.status(502).json({ error: 'Upstream architect unavailable', detail: err.message });
    }
  });
}

// ─── Job status (local) ──────────────────────────────────────────────────────
app.get('/api/jobs/:id', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json(job);
});

// ─── Mirror-import config ────────────────────────────────────────────────────
app.get('/api/mirror-import/config', (_req, res) => {
  res.json({
    deploymentSide: getDeploymentSide(),
    importPvcMountPath: process.env.IMPORT_PVC_MOUNT_PATH || '/import-data',
    targetRegistryDefaults: { url: process.env.TARGET_REGISTRY_URL || '' },
  });
});

// ─── List bundle files on mounted PVC ───────────────────────────────────────
app.get('/api/mirror-import/files', (_req, res) => {
  const mountPath = process.env.IMPORT_PVC_MOUNT_PATH || '/import-data';
  try {
    if (!fs.existsSync(mountPath)) return res.json({ files: [] });
    const entries = fs.readdirSync(mountPath).filter(f => f.endsWith('.tar') || f.endsWith('.tar.gz'));
    const files = entries.map(name => {
      const stat = fs.statSync(path.join(mountPath, name));
      return { name, size: stat.size, modified: stat.mtime.toISOString() };
    });
    res.json({ files });
  } catch (err) {
    logger.error({ error: err.message }, 'Failed to list import files');
    res.status(500).json({ error: err.message });
  }
});

// ─── Extract imageSetConfig from tar ────────────────────────────────────────
app.post('/api/mirror-import/extract-config', async (req, res) => {
  const { filename } = req.body || {};
  if (!filename) return res.status(400).json({ error: 'filename is required' });
  const mountPath = process.env.IMPORT_PVC_MOUNT_PATH || '/import-data';
  const filePath = path.join(mountPath, path.basename(filename));
  if (!fs.existsSync(filePath)) return res.status(404).json({ error: 'File not found on import volume' });
  try {
    const imageSetConfig = await extractImageSetConfigFromTar(filePath);
    res.json({ imageSetConfig: imageSetConfig || null });
  } catch (err) {
    logger.error({ error: err.message, filename }, 'Failed to extract imageset-config');
    res.status(500).json({ error: err.message });
  }
});

// ─── List PVCs ───────────────────────────────────────────────────────────────
app.get('/api/mirror-import/pvcs', async (_req, res) => {
  try {
    const pvcs = await listPvcs();
    res.json({ pvcs });
  } catch (err) {
    logger.error({ error: err.message }, 'Failed to list PVCs');
    res.status(500).json({ error: err.message });
  }
});

// ─── Chunked upload — init ───────────────────────────────────────────────────
app.post('/api/mirror-import/upload/init', async (req, res) => {
  const { filename, fileSize, chunkSize: reqChunkSize, pvcName, pvcSize, isNewPvc } = req.body || {};
  if (!filename || !fileSize) return res.status(400).json({ error: 'filename and fileSize are required' });

  const chunkSize = reqChunkSize || CHUNK_SIZE_DEFAULT;
  const totalChunks = Math.ceil(fileSize / chunkSize);
  const uploadKey = `${pvcName || 'local'}:${filename}`;
  const useUploadPod = !!pvcName;

  const existing = activeUploads.get(uploadKey);
  if (existing) {
    const existingSession = [...activeChunkedUploads.values()].find(s => s.jobId === existing);
    if (existingSession) {
      return res.status(202).json({
        uploadId: existingSession.uploadId, jobId: existing,
        totalChunks: existingSession.totalChunks, chunkSize: existingSession.chunkSize,
        receivedChunks: existingSession.receivedChunks.size,
      });
    }
    activeUploads.delete(uploadKey);
  }

  const uploadId = nanoid();
  const jobId = createJob('mirror-import-upload', `Uploading ${filename}`);
  updateJob(jobId, { status: 'running', progress: 0 });

  let podName = null, serviceName = null, podNamespace = null, pvcCreated = false;

  if (useUploadPod) {
    try {
      appendJobOutput(jobId, `Creating PVC ${pvcName} (${pvcSize})...\n`);
      await createPvc({ name: pvcName, size: pvcSize });
      pvcCreated = true;
      appendJobOutput(jobId, `Creating upload pod for PVC ${pvcName}...\n`);
      ({ podName, serviceName, namespace: podNamespace } = await createUploadPod({ pvcName }));
      appendJobOutput(jobId, 'Waiting for upload pod to be ready...\n');
      await waitForPodReady({ podName, namespace: podNamespace });
      appendJobOutput(jobId, 'Upload pod ready, accepting chunks.\n');
    } catch (err) {
      logger.error({ error: err.message, pvcName }, 'Failed to create upload pod');
      appendJobOutput(jobId, `Init failed: ${err.message}\n`);
      updateJob(jobId, { status: 'failed', progress: 0, message: err.message });
      if (podName && serviceName) await cleanupUploadPod({ podName, serviceName, namespace: podNamespace }).catch(() => {});
      if (pvcCreated && isNewPvc) await deletePvc({ name: pvcName, namespace: podNamespace }).catch(() => {});
      return res.status(500).json({ error: `Failed to initialize upload pod: ${err.message}` });
    }
  }

  let tmpDir = null;
  if (!useUploadPod) {
    tmpDir = path.join(dataDir, 'tmp', `chunked-${uploadId}`);
    fs.mkdirSync(tmpDir, { recursive: true });
  }

  const session = {
    uploadId, jobId, filename, fileSize, chunkSize, totalChunks,
    pvcName: pvcName || null, pvcSize: pvcSize || null, isNewPvc: !!isNewPvc,
    receivedChunks: new Set(), bytesReceived: 0, tmpDir,
    useUploadPod, podName, serviceName, podNamespace, uploadKey,
    createdAt: Date.now(), finalizing: false,
  };
  activeUploads.set(uploadKey, jobId);
  activeChunkedUploads.set(uploadId, session);
  res.status(202).json({ uploadId, jobId, totalChunks, chunkSize });
});

// ─── Chunked upload — chunk ──────────────────────────────────────────────────
app.put('/api/mirror-import/upload/:uploadId/chunk/:chunkIndex', async (req, res) => {
  const { uploadId, chunkIndex: chunkIndexStr } = req.params;
  const chunkIndex = parseInt(chunkIndexStr, 10);
  const session = activeChunkedUploads.get(uploadId);
  if (!session) return res.status(404).json({ error: 'Upload session not found' });
  if (session.finalizing) return res.status(409).json({ error: 'Upload is already finalizing' });
  if (isNaN(chunkIndex) || chunkIndex < 0 || chunkIndex >= session.totalChunks)
    return res.status(400).json({ error: 'chunkIndex out of range', max: session.totalChunks - 1 });
  if (session.receivedChunks.has(chunkIndex))
    return res.status(200).json({ chunkIndex, receivedChunks: session.receivedChunks.size, totalChunks: session.totalChunks });

  if (session.useUploadPod) {
    try {
      const contentLength = parseInt(req.headers['content-length'] || '0', 10);
      const result = await streamChunkToUploadPod({
        serviceName: session.serviceName, namespace: session.podNamespace,
        uploadId: session.uploadId, chunkIndex, reqStream: req, contentLength,
      });
      session.receivedChunks.add(chunkIndex);
      session.bytesReceived += result.written || 0;
      const progress = Math.min(45, Math.floor((session.receivedChunks.size / session.totalChunks) * 45));
      updateJob(session.jobId, { progress, message: `Receiving ${session.filename} (${session.receivedChunks.size}/${session.totalChunks} chunks)...` });
      res.status(200).json({ chunkIndex, bytesWritten: result.written || 0, receivedChunks: session.receivedChunks.size, totalChunks: session.totalChunks, progress });
    } catch (err) {
      logger.error({ uploadId, chunkIndex, error: err.message }, 'Failed to proxy chunk to upload pod');
      res.status(502).json({ error: 'Failed to stream chunk to upload pod', retryable: true });
    }
  } else {
    const chunkFile = path.join(session.tmpDir, `chunk-${String(chunkIndex).padStart(6, '0')}`);
    const ws = fs.createWriteStream(chunkFile);
    let chunkBytes = 0;
    req.on('data', buf => { chunkBytes += buf.length; });
    req.pipe(ws);
    ws.on('finish', () => {
      session.receivedChunks.add(chunkIndex);
      session.bytesReceived += chunkBytes;
      const progress = Math.min(45, Math.floor((session.receivedChunks.size / session.totalChunks) * 45));
      updateJob(session.jobId, { progress, message: `Receiving ${session.filename} (${session.receivedChunks.size}/${session.totalChunks} chunks)...` });
      res.status(200).json({ chunkIndex, bytesWritten: chunkBytes, receivedChunks: session.receivedChunks.size, totalChunks: session.totalChunks, progress });
    });
    ws.on('error', err => { logger.error({ uploadId, chunkIndex, error: err.message }, 'Failed to write chunk'); res.status(500).json({ error: 'Failed to write chunk' }); });
  }
});

// ─── Chunked upload — finalize ───────────────────────────────────────────────
app.post('/api/mirror-import/upload/:uploadId/finalize', (req, res) => {
  const { uploadId } = req.params;
  const session = activeChunkedUploads.get(uploadId);
  if (!session) return res.status(404).json({ error: 'Upload session not found' });
  if (session.finalizing) return res.status(202).json({ uploadId, jobId: session.jobId, status: 'already finalizing' });

  const missing = [];
  for (let i = 0; i < session.totalChunks; i++) { if (!session.receivedChunks.has(i)) missing.push(i); }
  if (missing.length > 0) return res.status(400).json({ error: 'Missing chunks', missing, received: session.receivedChunks.size, total: session.totalChunks });

  session.finalizing = true;
  res.status(202).json({ uploadId, jobId: session.jobId, filename: session.filename, status: 'assembling' });

  (async () => {
    try {
      if (session.useUploadPod) {
        updateJob(session.jobId, { progress: 46, message: `Assembling ${session.totalChunks} chunks on PVC...` });
        const result = await triggerAssembly({ serviceName: session.serviceName, namespace: session.podNamespace, uploadId: session.uploadId, filename: session.filename });
        if (result.imageSetConfig) updateJobMetadata(session.jobId, { imageSetConfig: result.imageSetConfig });
        await cleanupUploadPod({ podName: session.podName, serviceName: session.serviceName, namespace: session.podNamespace });
        updateJob(session.jobId, { status: 'completed', progress: 100, message: `Uploaded ${session.filename} to PVC ${session.pvcName}` });
        updateJobMetadata(session.jobId, { filename: session.filename, pvcName: session.pvcName, bytesWritten: result.written });
      } else {
        const assembledPath = path.join(session.tmpDir, `assembled-${session.filename}`);
        const outStream = fs.createWriteStream(assembledPath);
        for (let i = 0; i < session.totalChunks; i++) {
          const chunkFile = path.join(session.tmpDir, `chunk-${String(i).padStart(6, '0')}`);
          await new Promise((resolve, reject) => { const rs = fs.createReadStream(chunkFile); rs.pipe(outStream, { end: false }); rs.on('end', resolve); rs.on('error', reject); });
        }
        await new Promise((resolve, reject) => { outStream.end(); outStream.on('finish', resolve); outStream.on('error', reject); });
        const imageSetConfig = await extractImageSetConfigFromTar(assembledPath).catch(() => null);
        if (imageSetConfig) updateJobMetadata(session.jobId, { imageSetConfig });
        const mountPath = process.env.IMPORT_PVC_MOUNT_PATH || '/import-data';
        fs.mkdirSync(mountPath, { recursive: true });
        const destPath = path.join(mountPath, session.filename);
        await fs.promises.rename(assembledPath, destPath).catch(async () => { await fs.promises.copyFile(assembledPath, destPath); await fs.promises.unlink(assembledPath).catch(() => {}); });
        updateJob(session.jobId, { status: 'completed', progress: 100, message: `Uploaded ${session.filename}` });
        updateJobMetadata(session.jobId, { filename: session.filename, bytesWritten: session.bytesReceived });
      }
    } catch (err) {
      logger.error({ error: err.message, uploadId }, 'Finalize failed');
      updateJob(session.jobId, { status: 'failed', progress: 0, message: err.message });
    } finally {
      activeUploads.delete(session.uploadKey);
      activeChunkedUploads.delete(uploadId);
      if (session.tmpDir) fs.rm(session.tmpDir, { recursive: true, force: true }, () => {});
    }
  })();
});

// ─── Upload status ───────────────────────────────────────────────────────────
app.get('/api/mirror-import/upload/:uploadId/status', (req, res) => {
  const session = activeChunkedUploads.get(req.params.uploadId);
  if (!session) return res.status(404).json({ error: 'Upload session not found' });
  res.json({
    uploadId: session.uploadId, jobId: session.jobId,
    totalChunks: session.totalChunks, receivedChunks: session.receivedChunks.size,
    bytesReceived: session.bytesReceived, fileSize: session.fileSize,
    status: session.finalizing ? 'finalizing' : 'receiving',
  });
});

// ─── Legacy single-request upload ────────────────────────────────────────────
app.post('/api/mirror-import/upload', (req, res) => {
  const filename = req.query.filename || `import-${Date.now()}.tar`;
  const pvcName = req.query.pvcName || null;
  const contentLength = parseInt(req.headers['content-length'] || '0', 10);
  const uploadKey = `${pvcName || 'local'}:${filename}`;
  const existing = activeUploads.get(uploadKey);
  if (existing) return res.status(202).json({ jobId: existing, filename });

  const jobId = createJob('mirror-import-upload', `Uploading ${filename}`);
  updateJob(jobId, { status: 'running', progress: 0 });
  activeUploads.set(uploadKey, jobId);

  const tmpDir = path.join(dataDir, 'tmp');
  fs.mkdirSync(tmpDir, { recursive: true });
  const tmpPath = path.join(tmpDir, `upload-${jobId}-${filename}`);
  const tmpStream = fs.createWriteStream(tmpPath);
  let bytesReceived = 0;
  req.on('data', chunk => { bytesReceived += chunk.length; if (contentLength > 0) updateJob(jobId, { progress: Math.min(45, Math.floor((bytesReceived / contentLength) * 45)) }); });
  req.pipe(tmpStream);
  res.status(202).json({ jobId, filename });

  (async () => {
    try {
      await new Promise((resolve, reject) => { tmpStream.on('finish', resolve); tmpStream.on('error', reject); req.on('error', reject); });
      const mountPath = process.env.IMPORT_PVC_MOUNT_PATH || '/import-data';
      fs.mkdirSync(mountPath, { recursive: true });
      const destPath = path.join(mountPath, filename);
      await fs.promises.rename(tmpPath, destPath).catch(async () => { await fs.promises.copyFile(tmpPath, destPath); await fs.promises.unlink(tmpPath).catch(() => {}); });
      updateJob(jobId, { status: 'completed', progress: 100, message: `Uploaded ${filename}` });
      updateJobMetadata(jobId, { filename, bytesWritten: bytesReceived });
    } catch (err) {
      updateJob(jobId, { status: 'failed', progress: 0, message: err.message });
    } finally {
      activeUploads.delete(uploadKey);
      fs.unlink(tmpPath, () => {});
    }
  })();
});

// ─── Collections — download URL & proxy ──────────────────────────────────────
app.get('/api/collections/:name/download-url', async (req, res) => {
  try {
    const urls = await generateCollectionDownloadUrls({ collectionName: req.params.name, expiresIn: 3600 });
    if (Object.keys(urls).length === 0) return res.status(404).json({ error: `No artifacts found for collection: ${req.params.name}` });
    res.json({ collectionName: req.params.name, expiresIn: 3600, urls });
  } catch (err) {
    logger.error({ error: err.message }, 'Failed to generate download URLs');
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/collections/:name/download/:artifactType', async (req, res) => {
  const { name, artifactType } = req.params;
  if (!['bundle', 'signature'].includes(artifactType)) return res.status(400).json({ error: "artifactType must be 'bundle' or 'signature'" });
  try {
    const artifact = await resolveCollectionArtifact({ collectionName: name, artifactType });
    res.setHeader('Content-Type', artifact.contentType);
    res.setHeader('Content-Length', artifact.size);
    res.setHeader('Content-Disposition', `attachment; filename="${artifact.fileName}"`);
    const ac = new AbortController();
    req.on('close', () => ac.abort());
    await streamS3ParallelDownload({ s3Client: artifact.s3Client, bucket: artifact.bucket, key: artifact.key, totalSize: artifact.size, output: res, signal: ac.signal });
  } catch (err) {
    logger.error({ error: err.message }, 'Collection download failed');
    if (!res.headersSent) res.status(err.message.includes('not found') ? 404 : 500).json({ error: err.message });
  }
});

// ─── Health ───────────────────────────────────────────────────────────────────
app.get('/api/health', (_req, res) => res.json({ ok: true }));

app.listen(PORT, '127.0.0.1', () => {
  logger.info({ port: PORT }, 'Airgap Architect plugin backend listening');
});
