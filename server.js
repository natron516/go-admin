const express = require('express');
const basicAuth = require('express-basic-auth');
const multer = require('multer');

const app = express();
const PORT = process.env.PORT || 3000;

const MUX_TOKEN_ID  = process.env.MUX_TOKEN_ID  || '25cd1f0d-e6d4-445b-a106-e9ccc7a9f103';
const MUX_SECRET    = process.env.MUX_SECRET     || 'AcQYv3xI4uyhDIOmgAfaP+rAX9ei6bXzpT95dcAc74ALgOAl04BLg6o9PYwGh/iljlF4FTYz2VM';
const ADMIN_USER    = process.env.ADMIN_USER     || 'admin';
const ADMIN_PASS    = process.env.ADMIN_PASS     || 'gomedia';

const MUX_AUTH = Buffer.from(`${MUX_TOKEN_ID}:${MUX_SECRET}`).toString('base64');

const FB_API_KEY  = process.env.FB_API_KEY || 'AIzaSyAIP7-0-Ciop8tK0yOLwcSJhvwW6jPSKEo';
const FB_PROJECT  = 'gospel-outreach-tv';
const FIRESTORE   = `https://firestore.googleapis.com/v1/projects/${FB_PROJECT}/databases/(default)/documents`;

// Multer: memory storage, 800 KB limit
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 800 * 1024 } });

// ── Firestore helper (no auth — thumbnails collection is public) ────────────
async function fsGet(collection, docId) {
  const r = await fetch(`${FIRESTORE}/${collection}/${docId}?key=${FB_API_KEY}`);
  return r.json();
}
async function fsSet(collection, docId, fields) {
  // Build Firestore field map (strings only — avoids integerValue format issues)
  const fieldMap = {};
  for (const [k, v] of Object.entries(fields)) {
    fieldMap[k] = { stringValue: String(v) };
  }
  const r = await fetch(`${FIRESTORE}/${collection}/${docId}?key=${FB_API_KEY}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ fields: fieldMap }),
  });
  const json = await r.json();
  if (!r.ok) {
    console.error('Firestore error:', JSON.stringify(json));
    throw new Error(json?.error?.message || `Firestore ${r.status}`);
  }
  return json;
}
async function fsDelete(collection, docId) {
  await fetch(`${FIRESTORE}/${collection}/${docId}?key=${FB_API_KEY}`, { method: 'DELETE' });
}

// ── Auth ──────────────────────────────────────────────────────────────────────
app.use(basicAuth({ users: { [ADMIN_USER]: ADMIN_PASS }, challenge: true, realm: 'GO Admin' }));

app.use(express.json());
app.use(express.static('public'));

// ── Mux helper ────────────────────────────────────────────────────────────────
async function mux(method, path, body) {
  const res = await fetch(`https://api.mux.com${path}`, {
    method,
    headers: {
      Authorization: `Basic ${MUX_AUTH}`,
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  return res.json();
}

// Parse passthrough: supports plain string ("sermon") or JSON string
function parsePassthrough(raw) {
  try { return JSON.parse(raw || '{}'); } catch { return raw ? { category: raw } : {}; }
}

// Serialize passthrough back: if only has category and nothing else, keep as plain string
// for backwards compat. If has extra fields (thumbnail etc), use JSON.
function serializePassthrough(pt) {
  const keys = Object.keys(pt).filter(k => pt[k] != null && pt[k] !== '');
  if (keys.length === 1 && keys[0] === 'category') return pt.category;
  if (keys.length === 0) return '';
  return JSON.stringify(pt);
}

// ── Routes ────────────────────────────────────────────────────────────────────

// List all assets (recent 100)
app.get('/api/assets', async (req, res) => {
  try {
    const data = await mux('GET', '/video/v1/assets?limit=100&order_direction=desc');
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Create a direct upload URL
app.post('/api/create-upload', async (req, res) => {
  try {
    const { title, passthrough } = req.body;
    const data = await mux('POST', '/video/v1/uploads', {
      cors_origin: '*',
      new_asset_settings: {
        playback_policy: ['public'],
        meta: { title: title || 'Untitled' },
        passthrough: passthrough || '',
      },
    });
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Update asset metadata (title + category) — preserves thumbnail and other JSON keys
app.patch('/api/assets/:id', async (req, res) => {
  try {
    const { title, passthrough: category } = req.body;
    // Fetch current asset so we can preserve existing passthrough fields
    const current = await mux('GET', `/video/v1/assets/${req.params.id}`);
    const existing = parsePassthrough(current.data?.passthrough);
    existing.category = category || '';
    const data = await mux('PATCH', `/video/v1/assets/${req.params.id}`, {
      meta: { title: title || 'Untitled' },
      passthrough: serializePassthrough(existing),
    });
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Set or clear a custom thumbnail URL for an asset
app.patch('/api/assets/:id/thumbnail', async (req, res) => {
  try {
    const { thumbnailUrl } = req.body; // pass null/empty to clear
    const current = await mux('GET', `/video/v1/assets/${req.params.id}`);
    const pt = parsePassthrough(current.data?.passthrough);
    if (thumbnailUrl) {
      pt.thumbnail = thumbnailUrl;
    } else {
      delete pt.thumbnail;
    }
    const data = await mux('PATCH', `/video/v1/assets/${req.params.id}`, {
      passthrough: serializePassthrough(pt),
    });
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Debug: raw passthrough for all assets
app.get('/api/debug/passthrough', async (req, res) => {
  try {
    const data = await mux('GET', '/video/v1/assets?limit=100&order_direction=desc');
    const rows = (data.data || []).map(a => ({
      id: a.id,
      title: a.meta?.title,
      passthrough: a.passthrough,
    }));
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Delete an asset
app.delete('/api/assets/:id', async (req, res) => {
  try {
    await fetch(`https://api.mux.com/video/v1/assets/${req.params.id}`, {
      method: 'DELETE',
      headers: { Authorization: `Basic ${MUX_AUTH}` },
    });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// List live streams
app.get('/api/live-streams', async (req, res) => {
  try {
    const data = await mux('GET', '/video/v1/live-streams');
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Create a new live stream
app.post('/api/live-streams', async (req, res) => {
  try {
    const { title, category } = req.body;
    const passthrough = category || 'sermon';
    const data = await mux('POST', '/video/v1/live-streams', {
      playback_policy: ['public'],
      new_asset_settings: { playback_policy: ['public'] },
      meta: { title: title || 'Live Service' },
      passthrough,
      latency_mode: 'standard',
    });
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Update live stream passthrough (category)
app.patch('/api/live-streams/:id', async (req, res) => {
  try {
    const { category } = req.body;
    const data = await mux('PATCH', `/video/v1/live-streams/${req.params.id}`, {
      passthrough: category || '',
    });
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Thumbnail upload — stores image in Firestore, serves via this server ──────
// POST /api/thumbnails/upload  (multipart: field "image", query: assetId)
app.post('/api/thumbnails/upload', (req, res, next) => {
  upload.single('image')(req, res, (err) => {
    if (err) {
      console.error('Multer error:', err);
      return res.status(400).json({ error: err.message });
    }
    next();
  });
}, async (req, res) => {
  try {
    const assetId = req.query.assetId;
    if (!assetId) return res.status(400).json({ error: 'assetId required' });
    if (!req.file) return res.status(400).json({ error: 'No image file received. Make sure the field name is "image".' });
    console.log(`Thumbnail upload: assetId=${assetId} size=${req.file.size} type=${req.file.mimetype}`);
    const base64 = req.file.buffer.toString('base64');
    const contentType = req.file.mimetype || 'image/jpeg';
    await fsSet('thumbnails', assetId, { data: base64, contentType, assetId });
    const host = req.headers['x-forwarded-host'] || req.get('host');
    const proto = req.headers['x-forwarded-proto'] || req.protocol;
    const url = `${proto}://${host}/api/thumbnails/${assetId}`;
    console.log(`Thumbnail stored, url=${url}`);
    res.json({ url });
  } catch (e) {
    console.error('Upload handler error:', e);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/thumbnails/:assetId  — serve the stored image
app.get('/api/thumbnails/:assetId', async (req, res) => {
  try {
    const doc = await fsGet('thumbnails', req.params.assetId);
    if (doc.error || !doc.fields) return res.status(404).send('Not found');
    const base64 = doc.fields.data?.stringValue;
    const contentType = doc.fields.contentType?.stringValue || 'image/jpeg';
    if (!base64) return res.status(404).send('Not found');
    res.set('Content-Type', contentType);
    res.set('Cache-Control', 'public, max-age=31536000');
    res.send(Buffer.from(base64, 'base64'));
  } catch (e) {
    res.status(500).send('Error');
  }
});

// DELETE /api/thumbnails/:assetId
app.delete('/api/thumbnails/:assetId', async (req, res) => {
  try {
    await fsDelete('thumbnails', req.params.assetId);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () => console.log(`GO Admin running on :${PORT}`));
