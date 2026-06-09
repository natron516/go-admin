const ADMIN_BUILD = 55;
const crypto = require('crypto');
const express = require('express');
const basicAuth = require('express-basic-auth');
const multer = require('multer');
const sharp = require('sharp');
const admin = require('firebase-admin');

// ── Firebase Admin SDK ───────────────────────────────────
const sa = process.env.FIREBASE_SA ? JSON.parse(process.env.FIREBASE_SA) : null;
if (sa) {
  admin.initializeApp({ credential: admin.credential.cert(sa), storageBucket: 'gospel-outreach-tv.firebasestorage.app' });
  console.log('Firebase Admin SDK initialized');
} else {
  console.warn('FIREBASE_SA not set — user management disabled');
}

const app = express();
const PORT = process.env.PORT || 3000;

const MUX_TOKEN_ID  = process.env.MUX_TOKEN_ID  || '25cd1f0d-e6d4-445b-a106-e9ccc7a9f103';
const MUX_SECRET    = process.env.MUX_SECRET     || 'AcQYv3xI4uyhDIOmgAfaP+rAX9ei6bXzpT95dcAc74ALgOAl04BLg6o9PYwGh/iljlF4FTYz2VM';
const ADMIN_USER    = process.env.ADMIN_USER     || 'admin';
const ADMIN_PASS    = process.env.ADMIN_PASS     || 'gomedia';
const EDITOR_USER   = process.env.EDITOR_USER    || 'editor';
const EDITOR_PASS   = process.env.EDITOR_PASS    || 'goedit';

// Role-based users map (hardcoded + dynamic from Firestore)
const USERS = {
  [ADMIN_USER]:  { password: ADMIN_PASS,  role: 'admin' },
  [EDITOR_USER]: { password: EDITOR_PASS, role: 'editor' },
};

// Dynamic portal editors loaded from Firestore
const portalEditors = {}; // keyed by username (email) -> { password, role, uid, displayName }

function generatePassword() {
  return crypto.randomBytes(4).toString('hex'); // 8-char hex password
}

async function loadPortalEditors() {
  try {
    const db = admin.firestore();
    const snap = await db.collection('portalEditors').get();
    // Clear and reload
    Object.keys(portalEditors).forEach(k => delete portalEditors[k]);
    snap.forEach(doc => {
      const d = doc.data();
      if (d.username && d.password) {
        portalEditors[d.username] = { password: d.password, role: d.role || 'editor', uid: doc.id, displayName: d.displayName || '' };
      }
    });
    console.log(`Loaded ${Object.keys(portalEditors).length} portal editor(s)`);
  } catch (e) {
    console.error('Failed to load portal editors:', e.message);
  }
}

function lookupUser(username) {
  if (USERS[username]) return USERS[username];
  if (portalEditors[username]) return portalEditors[username];
  return null;
}

const MUX_AUTH = Buffer.from(`${MUX_TOKEN_ID}:${MUX_SECRET}`).toString('base64');

const FB_API_KEY  = process.env.FB_API_KEY || 'AIzaSyAIP7-0-Ciop8tK0yOLwcSJhvwW6jPSKEo';
const FB_PROJECT  = 'gospel-outreach-tv';
const FIRESTORE   = `https://firestore.googleapis.com/v1/projects/${FB_PROJECT}/databases/(default)/documents`;

// Multer: memory storage, 20 MB limit — sharp will compress it down
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

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
// HTML/static pages served publicly — login handled client-side.
// Only /api/* routes require auth (silent, no browser popup).
const silentAuth = basicAuth({
  users: { [ADMIN_USER]: ADMIN_PASS, [EDITOR_USER]: EDITOR_PASS },
  challenge: false,
  unauthorizedResponse: () => 'Unauthorized',
  authorizer: (username, password) => {
    const u = lookupUser(username);
    return u && basicAuth.safeCompare(password, u.password);
  },
  authorizeAsync: false,
});

// Attach role to request after auth
function attachRole(req, res, next) {
  const authHeader = req.headers.authorization || '';
  if (authHeader.startsWith('Basic ')) {
    const decoded = Buffer.from(authHeader.slice(6), 'base64').toString();
    const username = decoded.split(':')[0];
    const u = lookupUser(username);
    req.userRole = u?.role || 'editor';
    req.authUsername = username;
  } else {
    req.userRole = 'editor';
  }
  next();
}
app.use(attachRole);

// Middleware: block editors from destructive actions
function adminOnly(req, res, next) {
  if (req.userRole !== 'admin') return res.status(403).json({ error: 'Admin access required' });
  next();
}
app.use((req, res, next) => {
  // Public routes
  if (req.method === 'GET' && req.path.startsWith('/api/thumbnails/')) return next();
  if (req.path === '/webhooks/mux') return next();
  if (req.path === '/api/fcm-token' && req.method === 'POST') return next();
  if (req.path === '/api/build') return next();
  if (req.path === '/upload-test.html') return next();
  if (req.path === '/cast-receiver.html') return next();
  // Login endpoint: public (no auth needed to log in)
  if (req.path === '/api/login' && req.method === 'POST') return next();
  // HTML/static: serve publicly (login overlay handles auth)
  if (!req.path.startsWith('/api/')) return next();
  // API routes: silent basic auth (no WWW-Authenticate = no popup)
  silentAuth(req, res, next);
});

app.use(express.json());

// Login endpoint for the v2 client-side login overlay
app.post('/api/login', (req, res) => {
  const { username, password } = req.body || {};
  const u = lookupUser(username);
  if (u && u.password === password) {
    return res.json({ ok: true, role: u.role });
  }
  res.status(401).json({ error: 'Invalid credentials' });
});

// GET /api/role — returns the current user's role
app.get('/api/role', (req, res) => {
  res.json({ role: req.userRole });
});

// ── Portal Editor Management (admin only) ─────────────────────────────────
// GET /api/portal-editors — list all portal editors
app.get('/api/portal-editors', adminOnly, async (req, res) => {
  try {
    const db = admin.firestore();
    const snap = await db.collection('portalEditors').get();
    const editors = [];
    snap.forEach(doc => {
      const d = doc.data();
      editors.push({ uid: doc.id, username: d.username, displayName: d.displayName || '', role: d.role || 'editor', createdAt: d.createdAt || '' });
    });
    res.json({ editors });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/portal-editors — grant portal editor access to a user
app.post('/api/portal-editors', adminOnly, async (req, res) => {
  try {
    const { uid, email, displayName } = req.body;
    if (!uid || !email) return res.status(400).json({ error: 'uid and email required' });
    // Check if already exists
    const db = admin.firestore();
    const existing = await db.collection('portalEditors').doc(uid).get();
    if (existing.exists) return res.status(409).json({ error: 'User already has portal access' });
    const password = generatePassword();
    const data = { username: email.toLowerCase(), password, role: 'editor', displayName: displayName || '', createdAt: new Date().toISOString() };
    await db.collection('portalEditors').doc(uid).set(data);
    // Update in-memory cache
    portalEditors[data.username] = { password, role: 'editor', uid, displayName: data.displayName };
    res.json({ ok: true, username: data.username, password, role: 'editor' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// PATCH /api/portal-editors/:uid — reset password
app.patch('/api/portal-editors/:uid', adminOnly, async (req, res) => {
  try {
    const db = admin.firestore();
    const doc = await db.collection('portalEditors').doc(req.params.uid).get();
    if (!doc.exists) return res.status(404).json({ error: 'Not found' });
    const password = generatePassword();
    await db.collection('portalEditors').doc(req.params.uid).update({ password });
    const d = doc.data();
    if (portalEditors[d.username]) portalEditors[d.username].password = password;
    res.json({ ok: true, username: d.username, password });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/portal-editors/:uid — revoke portal access
app.delete('/api/portal-editors/:uid', adminOnly, async (req, res) => {
  try {
    const db = admin.firestore();
    const doc = await db.collection('portalEditors').doc(req.params.uid).get();
    if (doc.exists) {
      const d = doc.data();
      delete portalEditors[d.username];
    }
    await db.collection('portalEditors').doc(req.params.uid).delete();
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Mux Webhook ───────────────────────────────────────────────────────────────
// Receives events from Mux. Register this URL in the Mux dashboard:
//   https://go-admin-production-6be4.up.railway.app/webhooks/mux
app.post('/webhooks/mux', express.raw({ type: 'application/json' }), async (req, res) => {
  let event;
  try {
    event = JSON.parse(req.body.toString());
  } catch {
    return res.status(400).send('Bad JSON');
  }

  // Only handle the event fired when a live stream recording becomes a ready asset
  if (event.type === 'video.asset.live_stream_completed') {
    const asset = event.data;
    const assetId = asset.id;

    // Map live stream ID → display name
    const STREAM_NAMES = {
      // New 4K VOD streams (video_quality: plus)
      'ECgSydhoD601OoMqVmiNvfH6y0100m8uENxk6KKJV4NSZQ': 'Sermon',
      '9AE9TGtlF4mLcYyoXsUIVnxCtjTwTcdoN3fMOayKPQ00': 'Recitals',
      'BM009oOkP6xfixT3Qh9GgoHjx007GaOGEvRO02TpAOIfEo': 'School Events',
      // Old stream (kept until active stream ends)
      'J48167Z8yh011ZdcB4s6ou1jguYO4obCklJufOORrFVw': 'School Events',
    };

    // Build title: "Sermon – May 10, 2026" from stream key + asset creation date
    try {
      const pt = parsePassthrough(asset.passthrough);
      const streamId = asset.live_stream_id;
      const catLabel = STREAM_NAMES[streamId] || (() => {
        const cat = (pt.category || asset.passthrough || 'sermon').trim();
        return cat.replace(/\b\w/g, c => c.toUpperCase());
      })();
      const date = new Date(asset.created_at * 1000);
      const dateStr = date.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
      const title = `${catLabel} \u2013 ${dateStr}`;

      // Patch meta.title AND store title in passthrough JSON so the iOS app picks it up
      pt.title = title;

      // For Sermon streams on Sundays, pick a thumbnail frame from 10:03–10:07 AM Pacific
      // instead of the default 10 seconds into the stream
      const isSermon = catLabel === 'Sermon';
      const assetDate = new Date(asset.created_at * 1000);
      // Get day-of-week in Pacific time
      const pacificDay = new Date(assetDate.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' })).getDay();
      const isSunday = pacificDay === 0;
      if (isSermon && isSunday) {
        // Pick a thumbnail frame from 10:03–10:07 AM Pacific (when sermon starts)
        const targetMinute = 3 + Math.floor(Math.random() * 5); // 3,4,5,6, or 7
        const startPacific = new Date(assetDate.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }));
        const targetPacific = new Date(startPacific);
        targetPacific.setHours(10, targetMinute, 0, 0);
        const offsetSeconds = Math.round((targetPacific - startPacific) / 1000);
        if (offsetSeconds > 0 && offsetSeconds < (asset.duration || Infinity)) {
          pt.thumbnail_time = String(offsetSeconds);
          console.log(`[webhook] Sermon Sunday thumbnail: offset=${offsetSeconds}s (10:${String(targetMinute).padStart(2,'0')} AM Pacific)`);
        }
      }

      await mux('PATCH', `/video/v1/assets/${assetId}`, {
        meta: { title },
        passthrough: serializePassthrough(pt),
      });
      console.log(`[webhook] Auto-titled asset ${assetId}: "${title}"`);

      // Enable MP4 downloads on the new VOD asset
      if (asset.mp4_support !== 'standard') {
        try {
          await mux('PUT', `/video/v1/assets/${assetId}/mp4-support`, { mp4_support: 'standard' });
          console.log(`[webhook] Enabled mp4_support for ${assetId}`);
        } catch (e) {
          console.error(`[webhook] Failed to enable mp4_support: ${e.message}`);
        }
      }
    } catch (err) {
      console.error('[webhook] Failed to auto-title asset:', err.message);
    }
  }

  res.sendStatus(200);
});
app.get('/api/build', (req, res) => res.json({ build: ADMIN_BUILD }));
app.use(express.static('public', { maxAge: 0, etag: false, setHeaders: (res, path) => {
  if (path.endsWith('.html')) res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
}}));

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
// Track assets with MP4 generation in progress (server-side, survives client navigation)
const _mp4Preparing = {}; // { assetId: { title, startedAt } }

app.get('/api/assets', async (req, res) => {
  try {
    // Paginate through ALL Mux assets
    let allAssets = [];
    let page = 1;
    let cursor = null;
    while (true) {
      const url = cursor
        ? `/video/v1/assets?limit=100&order_direction=desc&page=${page}`
        : '/video/v1/assets?limit=100&order_direction=desc';
      const data = await mux('GET', cursor
        ? `/video/v1/assets?limit=100&order_direction=desc&cursor=${encodeURIComponent(cursor)}`
        : '/video/v1/assets?limit=100&order_direction=desc');
      if (data.data) allAssets.push(...data.data);
      if (data.next_cursor) {
        cursor = data.next_cursor;
        page++;
      } else {
        break;
      }
    }
    // Augment assets with server-tracked MP4 preparation state
    const now = Date.now();
    for (const a of allAssets) {
      if (_mp4Preparing[a.id]) {
        if (now - _mp4Preparing[a.id].startedAt > 10 * 60 * 1000) {
          delete _mp4Preparing[a.id];
        } else {
          a._mp4Preparing = true;
        }
      }
    }
    res.json({ data: allAssets });
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
        mp4_support: 'standard',
        meta: { title: title || 'Untitled' },
        passthrough: passthrough || '',
      },
    });
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Check upload status — returns playback URL when asset is ready
app.get('/api/upload-status/:uploadId', async (req, res) => {
  try {
    const data = await mux('GET', `/video/v1/uploads/${req.params.uploadId}`);
    const upload = data.data || {};
    const status = upload.status; // 'waiting', 'asset_created', 'cancelled', 'timed_out', 'errored'
    const assetId = upload.asset_id;
    if (status === 'asset_created' && assetId) {
      // Fetch the asset to get playback ID
      const assetData = await mux('GET', `/video/v1/assets/${assetId}`);
      const asset = assetData.data || {};
      const pid = asset.playback_ids?.[0]?.id;
      const assetStatus = asset.status; // 'preparing', 'ready', 'errored'
      if (pid && assetStatus === 'ready') {
        res.json({ status: 'ready', playbackId: pid, streamUrl: `https://stream.mux.com/${pid}.m3u8`, assetId });
      } else {
        res.json({ status: 'processing', assetStatus, assetId });
      }
    } else {
      res.json({ status: status || 'waiting' });
    }
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

// Get or enable MP4 download for an asset
app.post('/api/assets/:id/mp4', async (req, res) => {
  try {
    const current = await mux('GET', `/video/v1/assets/${req.params.id}`);
    const asset = current.data;
    if (!asset) return res.status(404).json({ error: 'Asset not found' });

    const pid = asset.playback_ids?.[0]?.id;
    if (!pid) return res.status(400).json({ error: 'No playback ID' });

    // If MP4 not enabled, enable it now and return preparing
    if (asset.mp4_support !== 'standard') {
      try {
        await mux('PUT', `/video/v1/assets/${req.params.id}/mp4-support`, { mp4_support: 'standard' });
        console.log(`[mp4] Enabled mp4_support for ${req.params.id}`);
        return res.json({ status: 'preparing', message: 'MP4 support enabled — generating renditions now. Try again in 1-2 minutes.' });
      } catch (e) {
        console.error(`[mp4] Failed to enable mp4_support: ${e.message}`);
        return res.json({ status: 'unavailable', message: 'Failed to enable MP4 for this asset: ' + e.message });
      }
    }

    const sr = asset.static_renditions;
    if (!sr || sr.status === 'preparing') {
      return res.json({ status: 'preparing', message: 'MP4 is still being generated.' });
    }
    if (sr.status === 'errored') {
      return res.json({ status: 'errored', message: 'MP4 generation failed.' });
    }

    // Ready — build download URLs from available files
    const files = (sr.files || []).map(f => ({
      name: f.name,
      ext: f.ext,
      width: f.width,
      height: f.height,
      bitrate: f.bitrate,
      url: `https://stream.mux.com/${pid}/${f.name}`,
    }));

    // Pick the highest quality file as the primary download
    const best = files.reduce((a, b) => ((b.height || 0) > (a.height || 0) ? b : a), files[0]);

    res.json({ status: 'ready', files, downloadUrl: best?.url || null, playbackId: pid });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Proxy MP4 download (browsers block cross-origin download attribute)
app.get('/api/assets/:id/mp4/download', async (req, res) => {
  try {
    const current = await mux('GET', `/video/v1/assets/${req.params.id}`);
    const asset = current.data;
    if (!asset) return res.status(404).json({ error: 'Asset not found' });
    const pid = asset.playback_ids?.[0]?.id;
    if (!pid) return res.status(400).json({ error: 'No playback ID' });
    const sr = asset.static_renditions;
    if (!sr || sr.status !== 'ready' || !sr.files?.length) {
      return res.status(404).json({ error: 'No MP4 renditions available' });
    }
    const quality = req.query.quality || 'high';
    const file = sr.files.find(f => f.name === `${quality}.mp4`) || sr.files[0];
    const muxUrl = `https://stream.mux.com/${pid}/${file.name}`;
    const title = (asset.passthrough || req.query.title || 'video').replace(/[^a-zA-Z0-9 _-]/g, '');
    res.setHeader('Content-Disposition', `attachment; filename="${title}.mp4"`);
    res.setHeader('Content-Type', 'video/mp4');
    const https = require('https');
    https.get(muxUrl, (upstream) => {
      if (upstream.headers['content-length']) res.setHeader('Content-Length', upstream.headers['content-length']);
      upstream.pipe(res);
    }).on('error', (e) => {
      res.status(502).json({ error: 'Failed to fetch from Mux: ' + e.message });
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Clip/trim an asset — creates a new asset from a time range of the original
app.post('/api/assets/:id/clip', async (req, res) => {
  try {
    const { startTime, endTime } = req.body;
    if (startTime == null || endTime == null) return res.status(400).json({ error: 'startTime and endTime required' });
    if (endTime <= startTime) return res.status(400).json({ error: 'endTime must be greater than startTime' });

    // Fetch original asset for title/category metadata
    const current = await mux('GET', `/video/v1/assets/${req.params.id}`);
    const asset = current.data;
    if (!asset) return res.status(404).json({ error: 'Asset not found' });

    const origTitle = asset.meta?.title || 'Untitled';
    const pt = parsePassthrough(asset.passthrough);

    // Create new asset from the clip
    const clipData = await mux('POST', '/video/v1/assets', {
      input: [{
        url: `mux://assets/${req.params.id}`,
        start_time: parseFloat(startTime),
        end_time: parseFloat(endTime),
      }],
      playback_policy: ['public'],
      mp4_support: 'standard',
      meta: { title: `${origTitle} (trimmed)` },
      passthrough: serializePassthrough(pt),
    });

    res.json({ ok: true, asset: clipData.data });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Delete an asset
app.delete('/api/assets/:id', adminOnly, async (req, res) => {
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
    const cat = category || 'sermon';
    // Auto-build title from category + date if not explicitly provided
    const dateStr = new Date().toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' });
    const catLabel = cat.charAt(0).toUpperCase() + cat.slice(1);
    const resolvedTitle = title || `${catLabel} – ${dateStr}`;
    // Store title in passthrough JSON so it carries over to the VOD asset after the stream ends.
    // Mux copies passthrough to the new asset automatically, but does NOT copy meta.title.
    const passthrough = JSON.stringify({ category: cat, title: resolvedTitle });
    const data = await mux('POST', '/video/v1/live-streams', {
      playback_policy: ['public'],
      new_asset_settings: { playback_policy: ['public'], video_quality: 'plus' },
      meta: { title: resolvedTitle },
      passthrough,
      latency_mode: 'standard',
    });
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Update live stream passthrough (category) — preserves title and other JSON keys
app.patch('/api/live-streams/:id', async (req, res) => {
  try {
    const { category } = req.body;
    const current = await mux('GET', `/video/v1/live-streams/${req.params.id}`);
    const existing = parsePassthrough(current.data?.passthrough);
    existing.category = category || '';
    const data = await mux('PATCH', `/video/v1/live-streams/${req.params.id}`, {
      passthrough: serializePassthrough(existing),
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
    // Resize to max 640px wide, compress to JPEG — keeps Firestore doc well under 1MB
    const compressed = await sharp(req.file.buffer)
      .resize({ width: 640, withoutEnlargement: true })
      .jpeg({ quality: 75 })
      .toBuffer();
    console.log(`Compressed: ${req.file.size} → ${compressed.length} bytes`);
    const base64 = compressed.toString('base64');
    const contentType = 'image/jpeg';
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

// Proxy external image for CORS-safe canvas use
app.get('/api/image-proxy', async (req, res) => {
  try {
    const url = req.query.url;
    if (!url) return res.status(400).json({ error: 'url required' });
    const https = require('https');
    const http = require('http');
    const mod = url.startsWith('https') ? https : http;
    mod.get(url, (upstream) => {
      if (upstream.statusCode !== 200) return res.status(upstream.statusCode).send('Upstream error');
      res.set('Content-Type', upstream.headers['content-type'] || 'image/jpeg');
      res.set('Cache-Control', 'no-store');
      upstream.pipe(res);
    }).on('error', (e) => res.status(502).json({ error: e.message }));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/thumbnails/:assetId
app.delete('/api/thumbnails/:assetId', adminOnly, async (req, res) => {
  try {
    await fsDelete('thumbnails', req.params.assetId);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Cover Image Upload — Firebase Storage for series/podcast/article covers ────
// POST /api/upload-cover  (multipart: field "image", query: type=series|podcast|article)
app.post('/api/upload-cover', upload.single('image'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No image file' });
    const type = req.query.type || 'general';
    const bucket = admin.storage().bucket();
    const ext = req.file.mimetype === 'image/png' ? 'png' : 'jpg';
    const filename = `covers/${type}_${Date.now()}_${Math.random().toString(36).slice(2,8)}.${ext}`;
    const file = bucket.file(filename);

    // Resize to max 800px wide, compress
    const compressed = await sharp(req.file.buffer)
      .resize({ width: 800, withoutEnlargement: true })
      .jpeg({ quality: 80 })
      .toBuffer();

    await file.save(compressed, {
      metadata: { contentType: 'image/jpeg' },
    });
    await file.makePublic();
    const url = `https://storage.googleapis.com/${bucket.name}/${filename}`;
    console.log(`Cover uploaded: type=${type} url=${url}`);
    res.json({ url });
  } catch (e) {
    console.error('[cover-upload] Error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── AI Placeholder Cover — generates a styled cover using sharp SVG overlay ────
app.post('/api/generate-cover', async (req, res) => {
  try {
    const { title, type } = req.body;
    if (!title) return res.status(400).json({ error: 'title required' });

    // Pick gradient colors based on type
    const palettes = {
      series:  ['#1a1a2e', '#16213e', '#0f3460'],
      podcast: ['#1b1b2f', '#162447', '#1f4068'],
      article: ['#2d132c', '#3a1c71', '#d76d77'],
      audiobook: ['#0d1b2a', '#1b263b', '#415a77'],
      default: ['#1a1a2e', '#16213e', '#e94560'],
    };
    const colors = palettes[type] || palettes.default;

    // Escape XML entities in title
    const safeTitle = String(title).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    // Word-wrap title (max ~20 chars per line)
    const words = safeTitle.split(' ');
    const lines = [];
    let cur = '';
    for (const w of words) {
      if (cur.length + w.length > 20 && cur) { lines.push(cur.trim()); cur = ''; }
      cur += w + ' ';
    }
    if (cur.trim()) lines.push(cur.trim());

    const fontSize = lines.some(l => l.length > 18) ? 32 : 38;
    const lineHeight = fontSize * 1.3;
    const startY = 400 - (lines.length * lineHeight) / 2;

    const textLines = lines.map((line, i) =>
      `<text x="400" y="${startY + i * lineHeight}" text-anchor="middle" font-family="Arial, Helvetica, sans-serif" font-size="${fontSize}" font-weight="bold" fill="white" filter="url(#shadow)">${line}</text>`
    ).join('\n');

    const typeLabel = (type || 'media').charAt(0).toUpperCase() + (type || 'media').slice(1);
    const icons = { series: '🎥', podcast: '🎧', article: '📰', audiobook: '📚' };
    const icon = icons[type] || '🎬';

    const svg = `<svg width="800" height="800" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" style="stop-color:${colors[0]}"/>
          <stop offset="50%" style="stop-color:${colors[1]}"/>
          <stop offset="100%" style="stop-color:${colors[2]}"/>
        </linearGradient>
        <filter id="shadow"><feDropShadow dx="2" dy="2" stdDeviation="3" flood-color="rgba(0,0,0,0.5)"/></filter>
      </defs>
      <rect width="800" height="800" fill="url(#bg)"/>
      <text x="400" y="180" text-anchor="middle" font-family="Arial" font-size="64">${icon}</text>
      ${textLines}
      <text x="400" y="700" text-anchor="middle" font-family="Arial, Helvetica, sans-serif" font-size="20" fill="rgba(255,255,255,0.4)" letter-spacing="4">${typeLabel.toUpperCase()}</text>
      <rect x="300" y="720" width="200" height="2" fill="rgba(255,255,255,0.15)"/>
    </svg>`;

    const imgBuffer = await sharp(Buffer.from(svg)).jpeg({ quality: 85 }).toBuffer();

    // Upload to Firebase Storage
    const bucket = admin.storage().bucket();
    const filename = `covers/${type || 'gen'}_${Date.now()}_${Math.random().toString(36).slice(2,8)}.jpg`;
    const file = bucket.file(filename);
    await file.save(imgBuffer, { metadata: { contentType: 'image/jpeg' } });
    await file.makePublic();
    const url = `https://storage.googleapis.com/${bucket.name}/${filename}`;
    console.log(`Generated cover: type=${type} title="${title}" url=${url}`);
    res.json({ url });
  } catch (e) {
    console.error('[generate-cover] Error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── App Feedback ────────────────────────────────

app.get('/api/feedback', async (req, res) => {
  try {
    const snap = await admin.firestore()
      .collection('feedback')
      .orderBy('timestamp', 'desc')
      .get();
    const feedback = snap.docs.map(doc => ({
      id: doc.id,
      ...doc.data(),
      timestamp: doc.data().timestamp?.toDate?.()?.toISOString() || null,
    }));
    res.json({ feedback });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/feedback/:id/read', async (req, res) => {
  try {
    await admin.firestore().collection('feedback').doc(req.params.id).update({ read: true });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/feedback/:id', adminOnly, async (req, res) => {
  try {
    await admin.firestore().collection('feedback').doc(req.params.id).delete();
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Analytics ──────────────────────────────────

// Per-user session stats (app usage time from Firestore)
app.get('/api/analytics/sessions', async (req, res) => {
  try {
    // Get sessions from last 30 days
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - 30);
    const snap = await admin.firestore()
      .collection('sessions')
      .where('startedAt', '>=', cutoff)
      .orderBy('startedAt', 'desc')
      .limit(500)
      .get();

    // Aggregate by user
    const users = {};
    snap.docs.forEach(doc => {
      const d = doc.data();
      if (!d.uid) return;
      if (!users[d.uid]) {
        users[d.uid] = { uid: d.uid, totalSeconds: 0, sessionCount: 0, lastActive: null, platform: d.platform || 'Unknown' };
      }
      const u = users[d.uid];
      u.totalSeconds += d.durationSeconds || 0;
      u.sessionCount += 1;
      const started = d.startedAt?.toDate?.();
      if (started && (!u.lastActive || started > u.lastActive)) {
        u.lastActive = started;
        u.platform = d.platform || u.platform;
      }
    });

    // Convert to array, sort by most active
    const result = Object.values(users)
      .map(u => ({ ...u, lastActive: u.lastActive?.toISOString() || null }))
      .sort((a, b) => b.totalSeconds - a.totalSeconds);

    res.json({ users: result });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Video stats from Mux Data — works with current app builds (no update needed)
app.get('/api/analytics/videos', async (req, res) => {
  try {
    const days = req.query.days || 90;
    const vidRes = await fetch(
      `https://api.mux.com/data/v1/metrics/views/breakdown?group_by=video_title&timeframe[]=${days}:days&limit=50`,
      { headers: { Authorization: `Basic ${MUX_AUTH}` } }
    );
    const data = await vidRes.json();
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Platform breakdown from Mux Data
app.get('/api/analytics/platforms', async (req, res) => {
  try {
    const days = req.query.days || 90;
    const platRes = await fetch(
      `https://api.mux.com/data/v1/metrics/views/breakdown?group_by=operating_system&timeframe[]=${days}:days`,
      { headers: { Authorization: `Basic ${MUX_AUTH}` } }
    );
    const data = await platRes.json();
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Overall view count over time
app.get('/api/analytics/overview', async (req, res) => {
  try {
    const days = req.query.days || 90;
    const overRes = await fetch(
      `https://api.mux.com/data/v1/metrics/views/timeseries?timeframe[]=${days}:days`,
      { headers: { Authorization: `Basic ${MUX_AUTH}` } }
    );
    const data = await overRes.json();
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Mux cost estimate from delivery usage API
app.get('/api/analytics/mux-cost', async (req, res) => {
  try {
    const now = Math.floor(Date.now() / 1000);
    const end = Math.floor((now / 3600) - 12) * 3600;
    const start = end - 2592000; // 30 days
    const muxRes = await fetch(
      `https://api.mux.com/video/v1/delivery-usage?timeframe[]=${start}&timeframe[]=${end}&limit=200`,
      { headers: { Authorization: `Basic ${MUX_AUTH}` } }
    );
    const muxData = await muxRes.json();
    let total720 = 0, total1080 = 0, total2160 = 0, totalSeconds = 0, storageHours = 0;
    (muxData.data || []).forEach(a => {
      totalSeconds += a.delivered_seconds || 0;
      const res = a.delivered_seconds_by_resolution || {};
      total720 += res.tier_720p || 0;
      total1080 += res.tier_1080p || 0;
      total2160 += res.tier_2160p || 0;
      storageHours += (a.asset_duration || 0) / 3600;
    });
    // Mux pay-as-you-go pricing estimates
    const deliveryCost = (total720/60 * 0.00067) + (total1080/60 * 0.0013) + (total2160/60 * 0.004);
    const storageCost = storageHours * 0.007;
    const totalCost = deliveryCost + storageCost;
    res.json({
      totalDeliveredMinutes: Math.round(totalSeconds / 60),
      delivery: { min720: Math.round(total720/60), min1080: Math.round(total1080/60), min2160: Math.round(total2160/60) },
      storageHours: Math.round(storageHours * 10) / 10,
      costs: {
        delivery: Math.round(deliveryCost * 100) / 100,
        storage: Math.round(storageCost * 100) / 100,
        total: Math.round(totalCost * 100) / 100
      },
      assetCount: (muxData.data || []).length
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Per-asset watch metrics from Mux Data (views + watch time per asset)
app.get('/api/analytics/asset-metrics', async (req, res) => {
  try {
    const days = req.query.days || 90;
    const muxRes = await fetch(
      `https://api.mux.com/data/v1/metrics/views/breakdown?group_by=video_id&timeframe[]=${days}:days&limit=500`,
      { headers: { Authorization: `Basic ${MUX_AUTH}` } }
    );
    const muxData = await muxRes.json();
    const results = (muxData.data || []).map(v => ({
      assetId: v.field,
      views: v.views || v.value || 0,
      watchMinutes: Math.round((v.total_watch_time || 0) / 60000),
    }));
    res.json({ data: results });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Sermon PIN ─────────────────────────────────────────────────

app.get('/api/config/pin', async (req, res) => {
  try {
    const doc = await admin.firestore().collection('config').doc('app').get();
    res.json({ pin: doc.data()?.sermon_pin || '' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/config/pin', async (req, res) => {
  try {
    const { pin } = req.body;
    if (!pin || !/^\d{4}$/.test(pin)) return res.status(400).json({ error: 'PIN must be exactly 4 digits' });
    await admin.firestore().collection('config').doc('app').set({ sermon_pin: pin, pin_changed_at: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    res.json({ ok: true, pin });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── User management (Firebase Auth) ────────────────────────────────

// List all users
app.get('/api/users', async (req, res) => {
  if (!sa) return res.status(503).json({ error: 'Firebase Admin not configured' });
  try {
    const users = [];
    let pageToken;
    do {
      const result = await admin.auth().listUsers(1000, pageToken);
      result.users.forEach(u => users.push({
        uid: u.uid,
        email: u.email || '',
        displayName: u.displayName || '',
        photoURL: u.photoURL || '',
        disabled: u.disabled,
        provider: u.providerData?.[0]?.providerId || 'email',
        createdAt: u.metadata.creationTime,
        lastSignIn: u.metadata.lastSignInTime,
      }));
      pageToken = result.pageToken;
    } while (pageToken);
    // Look up platform, app version, and total session time from sessions collection
    const platformsMap = {};
    const appVersionMap = {};
    const sessionTimeMap = {};
    const sessionCountMap = {};
    try {
      const sessSnap = await admin.firestore().collection('sessions')
        .limit(5000).get();
      sessSnap.forEach(doc => {
        const d = doc.data();
        if (!d.uid) return;
        if (d.platform) {
          if (!platformsMap[d.uid]) platformsMap[d.uid] = new Set();
          platformsMap[d.uid].add(d.platform);
        }
        if (d.appVersion) appVersionMap[d.uid] = d.appVersion;
        sessionTimeMap[d.uid] = (sessionTimeMap[d.uid] || 0) + (d.durationSeconds || 0);
        sessionCountMap[d.uid] = (sessionCountMap[d.uid] || 0) + 1;
      });
    } catch (e) {
      console.warn('Sessions lookup failed:', e.message);
    }

    // Get actual video watch time per viewer from Mux Data API
    const watchMap = {};
    try {
      const muxRes = await fetch(
        'https://api.mux.com/data/v1/metrics/views/breakdown?group_by=viewer_user_id&timeframe[]=90:days&limit=250',
        { headers: { Authorization: `Basic ${MUX_AUTH}` } }
      );
      const muxData = await muxRes.json();
      (muxData.data || []).forEach(v => {
        if (v.field) watchMap[v.field] = Math.round((v.total_watch_time || 0) / 60000);
      });
    } catch (e) {
      console.warn('Mux watch time lookup failed:', e.message);
    }

    // Look up privateAccess from Firestore users collection
    const privateMap = {};
    try {
      const privateSnap = await admin.firestore().collection('users').get();
      privateSnap.forEach(doc => {
        const d = doc.data();
        if (d.privateAccess) privateMap[doc.id] = true;
      });
    } catch (e) {
      console.warn('Private access lookup failed:', e.message);
    }

    users.forEach(u => {
      u.platforms = platformsMap[u.uid] ? [...platformsMap[u.uid]] : [];
      u.minutesWatched = watchMap[u.uid] || 0;
      u.privateAccess = !!privateMap[u.uid];
      u.appVersion = appVersionMap[u.uid] || null;
      u.appMinutes = Math.round((sessionTimeMap[u.uid] || 0) / 60);
      u.sessionCount = sessionCountMap[u.uid] || 0;
    });

    // Sort newest first
    users.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    res.json({ users });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// New signup notifications (polled by OpenClaw cron)
app.get('/api/notifications/new-signups', async (req, res) => {
  try {
    const snap = await admin.firestore().collection('newSignups')
      .where('notified', '==', false).limit(50).get();
    const signups = [];
    snap.forEach(d => signups.push({ id: d.id, ...d.data() }));
    res.json({ signups });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/notifications/new-signups/:uid/ack', async (req, res) => {
  try {
    await admin.firestore().collection('newSignups').doc(req.params.uid).update({ notified: true });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Debug: check sessions collection
app.get('/api/debug/sessions', async (req, res) => {
  try {
    const snap = await admin.firestore().collection('sessions').limit(10).get();
    const docs = [];
    snap.forEach(d => docs.push({ id: d.id, ...d.data() }));
    res.json({ count: snap.size, docs });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Block (disable) or unblock (enable) a user
app.patch('/api/users/:uid/block', adminOnly, async (req, res) => {
  if (!sa) return res.status(503).json({ error: 'Firebase Admin not configured' });
  try {
    const { blocked } = req.body; // true = block, false = unblock
    await admin.auth().updateUser(req.params.uid, { disabled: !!blocked });
    // Also write to Firestore so the app can detect it
    await admin.firestore().collection('users').doc(req.params.uid).set(
      { blocked: !!blocked, blockedAt: new Date().toISOString() },
      { merge: true }
    );
    res.json({ ok: true, uid: req.params.uid, disabled: !!blocked });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Grant or revoke private content access
app.patch('/api/users/:uid/private-access', async (req, res) => {
  if (!sa) return res.status(503).json({ error: 'Firebase Admin not configured' });
  try {
    const { access } = req.body; // true = grant, false = revoke
    await admin.firestore().collection('users').doc(req.params.uid).set(
      { privateAccess: !!access },
      { merge: true }
    );
    res.json({ ok: true, uid: req.params.uid, privateAccess: !!access });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Delete a user account
app.delete('/api/users/:uid', adminOnly, async (req, res) => {
  if (!sa) return res.status(503).json({ error: 'Firebase Admin not configured' });
  try {
    await admin.auth().deleteUser(req.params.uid);
    await admin.firestore().collection('users').doc(req.params.uid).delete().catch(() => {});
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── App Sessions ──────────────────────────────────────────────────────────────

// Active sessions (endedAt is null)
app.get('/api/sessions/active', async (req, res) => {
  try {
    const db = admin.firestore();
    const snap = await db.collection('sessions')
      .where('endedAt', '==', null)
      .get();
    const sessions = [];
    const uidSet = new Set();
    snap.forEach(doc => {
      const d = doc.data();
      sessions.push({ id: doc.id, ...d });
      if (d.uid) uidSet.add(d.uid);
    });

    // Enrich with Firebase Auth display names
    const userMap = {};
    const uids = [...uidSet];
    if (uids.length > 0) {
      // Batch lookup (max 100 at a time)
      for (let i = 0; i < uids.length; i += 100) {
        const batch = uids.slice(i, i + 100);
        const result = await admin.auth().getUsers(batch.map(uid => ({ uid })));
        result.users.forEach(u => {
          userMap[u.uid] = { displayName: u.displayName || '', email: u.email || '' };
        });
      }
    }

    const enriched = sessions.map(s => ({
      ...s,
      userDisplayName: s.displayName || userMap[s.uid]?.displayName || '',
      userEmail: userMap[s.uid]?.email || '',
      startedAt: s.startedAt?._seconds ? new Date(s.startedAt._seconds * 1000).toISOString() : s.startedAt,
      watchingSince: s.watchingSince?._seconds ? new Date(s.watchingSince._seconds * 1000).toISOString() : s.watchingSince,
    }));

    res.json({ sessions: enriched });
  } catch (e) {
    console.error('[Sessions Active] Error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// Recent completed sessions (last 50)
app.get('/api/sessions/recent', async (req, res) => {
  try {
    const db = admin.firestore();
    const snap = await db.collection('sessions')
      .orderBy('startedAt', 'desc')
      .limit(50)
      .get();
    const sessions = [];
    const uidSet = new Set();
    snap.forEach(doc => {
      const d = doc.data();
      sessions.push({ id: doc.id, ...d });
      if (d.uid) uidSet.add(d.uid);
    });

    // Enrich with Firebase Auth display names
    const userMap = {};
    const uids = [...uidSet];
    if (uids.length > 0) {
      for (let i = 0; i < uids.length; i += 100) {
        const batch = uids.slice(i, i + 100);
        const result = await admin.auth().getUsers(batch.map(uid => ({ uid })));
        result.users.forEach(u => {
          userMap[u.uid] = { displayName: u.displayName || '', email: u.email || '' };
        });
      }
    }

    const enriched = sessions.map(s => ({
      ...s,
      userDisplayName: s.displayName || userMap[s.uid]?.displayName || '',
      userEmail: userMap[s.uid]?.email || '',
      startedAt: s.startedAt?._seconds ? new Date(s.startedAt._seconds * 1000).toISOString() : s.startedAt,
      endedAt: s.endedAt?._seconds ? new Date(s.endedAt._seconds * 1000).toISOString() : s.endedAt,
    }));

    res.json({ sessions: enriched });
  } catch (e) {
    console.error('[Sessions Recent] Error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── Force Update Config ──────────────────────────────────────────────────────

app.get('/api/config/force-update', async (req, res) => {
  try {
    const doc = await admin.firestore().collection('config').doc('app').get();
    const data = doc.data() || {};
    res.json({
      minimumVersion: data.minimumVersion || '',
      recommendedVersion: data.recommendedVersion || '',
      updateMessage: data.updateMessage || '',
      forceUpdateEnabled: data.forceUpdateEnabled !== false, // default true
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/config/force-update', async (req, res) => {
  try {
    const { minimumVersion, recommendedVersion, updateMessage, forceUpdateEnabled } = req.body;
    const update = {};
    if (minimumVersion !== undefined) update.minimumVersion = minimumVersion;
    if (recommendedVersion !== undefined) update.recommendedVersion = recommendedVersion;
    if (updateMessage !== undefined) update.updateMessage = updateMessage;
    if (forceUpdateEnabled !== undefined) update.forceUpdateEnabled = !!forceUpdateEnabled;
    await admin.firestore().collection('config').doc('app').set(update, { merge: true });
    res.json({ ok: true, ...update });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Version Distribution (from sessions) ─────────────────────────────────────

app.get('/api/analytics/version-distribution', async (req, res) => {
  try {
    const snap = await admin.firestore().collection('sessions').limit(5000).get();
    const versions = {};
    snap.forEach(doc => {
      const d = doc.data();
      const v = d.appVersion || 'Unknown';
      versions[v] = (versions[v] || 0) + 1;
    });
    // Sort by count desc
    const sorted = Object.entries(versions)
      .map(([version, count]) => ({ version, count }))
      .sort((a, b) => b.count - a.count);
    res.json({ versions: sorted });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── FCM Token Debug ─────────────────────────────────────────────────────────
let fcmTokens = {}; // device → { token, time, badge }
app.post('/api/fcm-token', (req, res) => {
  const { token, device } = req.body;
  if (token && !token.startsWith('TOKEN_FETCH_FAIL') && !token.startsWith('APNS_')) {
    const existing = fcmTokens[device || 'unknown'];
    fcmTokens[device || 'unknown'] = { token, time: new Date().toISOString(), badge: existing?.badge || 0 };
    console.log(`[FCM] Token registered: ${token.slice(0, 30)}... device=${device || 'unknown'}`);
  } else if (token) {
    console.log(`[FCM] Debug report: ${token.slice(0, 60)} device=${device || 'unknown'}`);
  }
  res.json({ ok: true, count: Object.keys(fcmTokens).length });
});
app.get('/api/fcm-tokens', (req, res) => res.json({ tokens: fcmTokens }));

// Reset badge count for a device (called when app opens)
app.post('/api/badge-reset', (req, res) => {
  const { device } = req.body;
  const key = device || 'unknown';
  if (fcmTokens[key]) {
    fcmTokens[key].badge = 0;
    console.log(`[FCM] Badge reset for device=${key}`);
  }
  res.json({ ok: true });
});

// Send notification directly to a specific device token
app.post('/api/notify-direct', async (req, res) => {
  if (!sa) return res.status(503).json({ error: 'Firebase Admin not configured' });
  try {
    const { title, body } = req.body;
    // Send to all registered device tokens
    const tokens = Object.values(fcmTokens).map(t => t.token);
    if (!tokens.length) return res.status(400).json({ error: 'No device tokens registered' });
    const results = [];
    for (const [deviceName, entry] of Object.entries(fcmTokens)) {
      try {
        entry.badge = (entry.badge || 0) + 1;
        const r = await admin.messaging().send({
          token: entry.token,
          notification: { title: title || 'GO Media', body: body || 'New video!' },
          apns: { payload: { aps: { sound: 'default', badge: entry.badge } } },
        });
        results.push({ ok: true, messageId: r, device: deviceName, badge: entry.badge });
      } catch (e) {
        results.push({ ok: false, error: e.message, device: deviceName });
      }
    }
    res.json({ results });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Push Notifications ────────────────────────────────────────────────────────

// Send a push notification to all users subscribed to the "new_video" topic
// Supports all content types: video, podcast, audiobook, audio, series, article
app.post('/api/notify', async (req, res) => {
  if (!sa) return res.status(503).json({ error: 'Firebase Admin not configured' });
  try {
    const { title, body, assetId, playbackId, thumbnailUrl, contentType, contentId } = req.body;
    if (!title || !body) return res.status(400).json({ error: 'title and body required' });

    const type = contentType || 'new_video';

    const message = {
      topic: 'new_video',
      notification: {
        title,
        body,
      },
      data: {
        type,
        assetId: assetId || '',
        playbackId: playbackId || '',
        contentId: contentId || '',
        contentType: type,
      },
      apns: {
        payload: {
          aps: {
            sound: 'default',
          },
        },
        fcm_options: {
          image: thumbnailUrl || '',
        },
      },
    };

    // Increment badge for all registered devices
    for (const entry of Object.values(fcmTokens)) {
      entry.badge = (entry.badge || 0) + 1;
    }

    const result = await admin.messaging().send(message);
    console.log(`[notify] Sent ${type} notification to new_video topic: ${result}`);
    res.json({ ok: true, messageId: result });
  } catch (e) {
    console.error('[notify] Error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── Books ────────────────────────────────────────────────────────────────────

app.get('/api/books', async (req, res) => {
  try {
    const db = admin.firestore();
    const snap = await db.collection('books').orderBy('sortOrder', 'asc').get();
    const books = [];
    snap.forEach(doc => books.push({ id: doc.id, ...doc.data() }));
    res.json({ books });
  } catch (e) {
    // Try without ordering if no index
    try {
      const db = admin.firestore();
      const snap = await db.collection('books').get();
      const books = [];
      snap.forEach(doc => books.push({ id: doc.id, ...doc.data() }));
      res.json({ books });
    } catch (e2) { res.status(500).json({ error: e2.message }); }
  }
});

app.post('/api/books', async (req, res) => {
  try {
    const db = admin.firestore();
    const { title, author, description, coverImageUrl, category, amazonUrl, kindleUrl, audiobookUrl, featured, sortOrder } = req.body;
    if (!title) return res.status(400).json({ error: 'title required' });
    const data = {
      title,
      author: author || '',
      description: description || '',
      coverImageUrl: coverImageUrl || '',
      category: category || '',
      amazonUrl: amazonUrl || '',
      kindleUrl: kindleUrl || '',
      audiobookUrl: audiobookUrl || '',
      featured: !!featured,
      sortOrder: Number(sortOrder) || 0,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    const ref = await db.collection('books').add(data);
    res.json({ id: ref.id, ...data });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/books/:id', async (req, res) => {
  try {
    const db = admin.firestore();
    const { title, author, description, coverImageUrl, category, amazonUrl, kindleUrl, audiobookUrl, featured, sortOrder } = req.body;
    const update = { updatedAt: admin.firestore.FieldValue.serverTimestamp() };
    if (title !== undefined) update.title = title;
    if (author !== undefined) update.author = author;
    if (description !== undefined) update.description = description;
    if (coverImageUrl !== undefined) update.coverImageUrl = coverImageUrl;
    if (category !== undefined) update.category = category;
    if (amazonUrl !== undefined) update.amazonUrl = amazonUrl;
    if (kindleUrl !== undefined) update.kindleUrl = kindleUrl;
    if (audiobookUrl !== undefined) update.audiobookUrl = audiobookUrl;
    if (featured !== undefined) update.featured = !!featured;
    if (sortOrder !== undefined) update.sortOrder = Number(sortOrder);
    await db.collection('books').doc(req.params.id).update(update);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/books/:id', adminOnly, async (req, res) => {
  try {
    await admin.firestore().collection('books').doc(req.params.id).delete();
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Book Search (Google Books API proxy) ─────────────────────────────────────
app.get('/api/books/search', async (req, res) => {
  const q = req.query.q;
  if (!q) return res.json({ results: [] });
  try {
    // Use Open Library API (free, no rate limits) instead of Google Books
    const r = await fetch(`https://openlibrary.org/search.json?q=${encodeURIComponent(q)}&limit=8&fields=title,author_name,first_sentence,isbn,cover_i,number_of_pages_median,first_publish_year,subject`);
    const data = await r.json();
    const results = (data.docs || []).map(doc => {
      const isbns = doc.isbn || [];
      const isbn13 = isbns.find(i => i.length === 13) || '';
      const isbn10 = isbns.find(i => i.length === 10) || '';
      const isbn = isbn10 || isbn13;
      const coverId = doc.cover_i;
      const coverUrl = coverId ? `https://covers.openlibrary.org/b/id/${coverId}-L.jpg` : '';
      const title = doc.title || '';
      const author = (doc.author_name || []).join(', ');
      const firstSentence = Array.isArray(doc.first_sentence) ? doc.first_sentence[0] : (doc.first_sentence || '');
      return {
        title,
        author,
        description: firstSentence,
        coverImageUrl: coverUrl,
        amazonUrl: isbn ? `https://www.amazon.com/dp/${isbn}` : `https://www.amazon.com/s?k=${encodeURIComponent(title)}`,
        kindleUrl: `https://www.amazon.com/s?k=${encodeURIComponent(title + ' ' + author)}&i=digital-text`,
        audiobookUrl: `https://www.audible.com/search?keywords=${encodeURIComponent(title + ' ' + author)}`,
        pageCount: doc.number_of_pages_median || 0,
        publishedDate: doc.first_publish_year ? String(doc.first_publish_year) : '',
        categories: doc.subject ? doc.subject.slice(0, 5) : [],
      };
    });
    res.json({ results });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Music (Apple Music curated albums) ────────────────────────────────────────

app.get('/api/music', async (req, res) => {
  try {
    const db = admin.firestore();
    const doc = await db.collection('config').doc('music').get();
    const data = doc.exists ? doc.data() : {};
    res.json({ albums: data.albums || [], playlists: data.playlists || [] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/music/album', async (req, res) => {
  try {
    const { albumId, title, artist, type } = req.body;
    if (!albumId) return res.status(400).json({ error: 'albumId required' });
    const db = admin.firestore();
    const ref = db.collection('config').doc('music');
    const doc = await ref.get();
    const data = doc.exists ? doc.data() : { albums: [], playlists: [] };
    const albums = data.albums || [];
    if (albums.find(a => a.albumId === albumId)) return res.status(409).json({ error: 'Album already added' });
    const artworkUrl = req.body.artworkUrl || '';
    albums.push({ albumId, title: title || '', artist: artist || '', type: type || 'album', artworkUrl, addedAt: new Date().toISOString() });
    await ref.set({ ...data, albums }, { merge: true });
    res.json({ ok: true, albums });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/music/album/:albumId', adminOnly, async (req, res) => {
  try {
    const db = admin.firestore();
    const ref = db.collection('config').doc('music');
    const doc = await ref.get();
    if (!doc.exists) return res.json({ ok: true });
    const data = doc.data();
    data.albums = (data.albums || []).filter(a => a.albumId !== req.params.albumId);
    await ref.set(data);
    res.json({ ok: true, albums: data.albums });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/music/album/:albumId', async (req, res) => {
  try {
    const db = admin.firestore();
    const ref = db.collection('config').doc('music');
    const doc = await ref.get();
    if (!doc.exists) return res.status(404).json({ error: 'No music config' });
    const data = doc.data();
    const albums = data.albums || [];
    const idx = albums.findIndex(a => a.albumId === req.params.albumId);
    if (idx < 0) return res.status(404).json({ error: 'Album not found' });
    Object.assign(albums[idx], req.body);
    await ref.set({ ...data, albums });
    res.json({ ok: true, albums });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Music Search (iTunes Search API proxy) ───────────────────────────────────
app.get('/api/music/search', async (req, res) => {
  const q = req.query.q;
  if (!q) return res.json({ results: [] });
  try {
    const entity = req.query.entity || 'album';
    const r = await fetch(`https://itunes.apple.com/search?term=${encodeURIComponent(q)}&media=music&entity=${entity}&limit=15`);
    const data = await r.json();
    const results = (data.results || []).map(item => ({
      albumId: String(item.collectionId || item.trackId || ''),
      title: item.collectionName || item.trackName || '',
      artist: item.artistName || '',
      artworkUrl: (item.artworkUrl100 || '').replace('100x100', '600x600'),
      type: item.collectionType === 'Album' ? 'album' : 'single',
      trackCount: item.trackCount || 0,
      releaseDate: item.releaseDate || '',
      genre: item.primaryGenreName || '',
      collectionViewUrl: item.collectionViewUrl || '',
    }));
    const seen = new Set();
    const unique = results.filter(r => {
      if (!r.albumId || seen.has(r.albumId)) return false;
      seen.add(r.albumId);
      return true;
    });
    res.json({ results: unique });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Music Playlists CRUD ─────────────────────────────────────────────────────
app.get('/api/music/playlists', async (req, res) => {
  try {
    const db = admin.firestore();
    const ref = db.collection('config').doc('music');
    const doc = await ref.get();
    const data = doc.exists ? doc.data() : {};
    res.json({ playlists: data.playlists || [] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/music/playlist', async (req, res) => {
  try {
    const { playlistId, title, curatorName, artworkUrl, description } = req.body;
    if (!playlistId) return res.status(400).json({ error: 'playlistId required' });
    const db = admin.firestore();
    const ref = db.collection('config').doc('music');
    const doc = await ref.get();
    const data = doc.exists ? doc.data() : { albums: [], playlists: [] };
    const playlists = data.playlists || [];
    if (playlists.some(p => p.playlistId === playlistId)) {
      return res.status(409).json({ error: 'Playlist already added' });
    }
    playlists.push({
      playlistId,
      title: title || '',
      curatorName: curatorName || '',
      artworkUrl: artworkUrl || '',
      description: description || '',
      addedAt: new Date().toISOString(),
    });
    await ref.set({ ...data, playlists });
    res.json({ ok: true, playlists });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/music/playlist/:playlistId', adminOnly, async (req, res) => {
  try {
    const db = admin.firestore();
    const ref = db.collection('config').doc('music');
    const doc = await ref.get();
    if (!doc.exists) return res.json({ ok: true });
    const data = doc.data();
    data.playlists = (data.playlists || []).filter(p => p.playlistId !== req.params.playlistId);
    await ref.set(data);
    res.json({ ok: true, playlists: data.playlists });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Music Playlist Search (iTunes Search API) ────────────────────────────────
app.get('/api/music/search-playlists', async (req, res) => {
  const q = req.query.q;
  if (!q) return res.json({ results: [] });
  try {
    // iTunes doesn't have a direct playlist search, but we can search for playlists
    // by using the Apple Music catalog search via storefront
    // Alternative: use iTunes search with entity=musicArtist then build curated lists
    // For now, we search Apple Music's catalog endpoint
    const r = await fetch(`https://itunes.apple.com/search?term=${encodeURIComponent(q)}&media=music&entity=musicArtist&limit=10`);
    // Actually, iTunes API doesn't expose playlists directly.
    // We'll use a workaround: search the Apple Music web catalog
    const catalogR = await fetch(`https://api.music.apple.com/v1/catalog/us/search?types=playlists&term=${encodeURIComponent(q)}&limit=15`, {
      headers: { 'Authorization': 'Bearer ' + (process.env.APPLE_MUSIC_TOKEN || '') }
    }).catch(() => null);
    
    if (catalogR && catalogR.ok) {
      const data = await catalogR.json();
      const playlists = (data.results?.playlists?.data || []).map(p => ({
        playlistId: p.id,
        title: p.attributes?.name || '',
        curatorName: p.attributes?.curatorName || '',
        artworkUrl: (p.attributes?.artwork?.url || '').replace('{w}', '600').replace('{h}', '600'),
        description: p.attributes?.description?.short || '',
      }));
      return res.json({ results: playlists });
    }
    
    // Fallback: return empty — admin can add by ID manually
    res.json({ results: [], note: 'Apple Music API token not configured — add playlists by ID' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Seed music config with hardcoded album IDs (one-time migration)
app.post('/api/music/seed', async (req, res) => {
  try {
    const db = admin.firestore();
    const ref = db.collection('config').doc('music');
    const doc = await ref.get();
    if (doc.exists && (doc.data().albums || []).length > 0) {
      return res.json({ ok: true, message: 'Already seeded', count: doc.data().albums.length });
    }
    const defaultAlbums = [
      { albumId: '1887383546', title: 'Your Word Is a Lamp', type: 'album' },
      { albumId: '1841667676', title: 'Gospel Outreach Scripture Songs', type: 'album' },
      { albumId: '1831598367', title: 'On the Carousel', type: 'album' },
      { albumId: '1811597280', title: 'Sing Choirs of Angels', type: 'album' },
      { albumId: '1750051715', title: 'Lullabies', type: 'album' },
      { albumId: '1811680979', title: 'I Am Always with You', type: 'album' },
      { albumId: '1750085812', title: 'New Mercies', type: 'album' },
      { albumId: '6766344106', title: 'I Am Pressing On', type: 'single' },
      { albumId: '1889967963', title: 'My Father\'s World', type: 'single' },
      { albumId: '1882054119', title: 'Great Is the Lord', type: 'single' },
      { albumId: '724376682', title: 'Far Away Places', type: 'album' },
      { albumId: '724482211', title: 'Hymns II', type: 'album' },
      { albumId: '724693225', title: 'Hymns Instrumental', type: 'album' },
      { albumId: '724606076', title: 'Night Light', type: 'album' },
      { albumId: '715990922', title: 'The Roar of Love', type: 'album' },
      { albumId: '723893734', title: 'Singer Sower', type: 'album' },
      { albumId: '1167738867', title: 'Encores', type: 'album' },
      { albumId: '724642282', title: 'Rejoice', type: 'album' },
      { albumId: '724076458', title: 'Mansion Builder', type: 'album' },
      { albumId: '715917383', title: 'How the West Was One', type: 'album' },
      { albumId: '1167728908', title: 'To the Bride', type: 'album' },
      { albumId: '1841979336', title: 'In the Volume of the Book', type: 'album' },
      { albumId: '1841979100', title: 'With Footnotes', type: 'album' },
    ].map(a => ({ ...a, artist: '', addedAt: new Date().toISOString() }));
    await ref.set({ albums: defaultAlbums, playlists: [] });
    res.json({ ok: true, message: 'Seeded', count: defaultAlbums.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Articles ──────────────────────────────────────────────────────────────────

app.get('/api/articles', async (req, res) => {
  try {
    const db = admin.firestore();
    const snap = await db.collection('articles').orderBy('sortOrder', 'asc').get();
    const articles = [];
    snap.forEach(doc => articles.push({ id: doc.id, ...doc.data() }));
    res.json({ articles });
  } catch (e) {
    try {
      const db = admin.firestore();
      const snap = await db.collection('articles').get();
      const articles = [];
      snap.forEach(doc => articles.push({ id: doc.id, ...doc.data() }));
      res.json({ articles });
    } catch (e2) { res.status(500).json({ error: e2.message }); }
  }
});

// Upload PDF for articles — stores in Firebase Storage, returns public URL
app.post('/api/articles/upload-pdf', upload.single('pdf'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No PDF file' });
    if (req.file.size > 20 * 1024 * 1024) return res.status(400).json({ error: 'PDF too large (max 20MB)' });
    
    // Upload to Firebase Storage
    const bucket = admin.storage().bucket();
    const filename = `articles/pdf_${Date.now()}_${req.file.originalname.replace(/[^a-zA-Z0-9._-]/g, '_')}`;
    const file = bucket.file(filename);
    
    await file.save(req.file.buffer, {
      metadata: {
        contentType: 'application/pdf',
        metadata: { originalName: req.file.originalname },
      },
    });
    
    // Make publicly accessible
    await file.makePublic();
    const pdfUrl = `https://storage.googleapis.com/${bucket.name}/${filename}`;
    
    res.json({ pdfUrl, size: req.file.size, name: req.file.originalname });
  } catch (e) {
    console.error('[pdf-upload] Error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/articles', async (req, res) => {
  try {
    const db = admin.firestore();
    const { title, author, content, excerpt, coverImageUrl, category, published, featured, sortOrder, pdfUrl } = req.body;
    if (!title) return res.status(400).json({ error: 'title required' });
    const data = {
      title,
      author: author || '',
      content: content || '',
      excerpt: excerpt || '',
      coverImageUrl: coverImageUrl || '',
      category: category || 'articles',
      published: !!published,
      featured: !!featured,
      sortOrder: Number(sortOrder) || 0,
      pdfUrl: pdfUrl || '',
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    const ref = await db.collection('articles').add(data);
    res.json({ id: ref.id, ...data });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/articles/:id', async (req, res) => {
  try {
    const db = admin.firestore();
    const { title, author, content, excerpt, coverImageUrl, category, published, featured, sortOrder, pdfUrl } = req.body;
    const update = { updatedAt: admin.firestore.FieldValue.serverTimestamp() };
    if (title !== undefined) update.title = title;
    if (author !== undefined) update.author = author;
    if (content !== undefined) update.content = content;
    if (excerpt !== undefined) update.excerpt = excerpt;
    if (coverImageUrl !== undefined) update.coverImageUrl = coverImageUrl;
    if (category !== undefined) update.category = category;
    if (published !== undefined) update.published = !!published;
    if (featured !== undefined) update.featured = !!featured;
    if (sortOrder !== undefined) update.sortOrder = Number(sortOrder);
    if (pdfUrl !== undefined) update.pdfUrl = pdfUrl;
    await db.collection('articles').doc(req.params.id).update(update);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/articles/:id', adminOnly, async (req, res) => {
  try {
    await admin.firestore().collection('articles').doc(req.params.id).delete();
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Podcasts ──────────────────────────────────────────────────────────────────

app.get('/api/podcasts', async (req, res) => {
  try {
    const db = admin.firestore();
    const snap = await db.collection('podcasts').orderBy('sortOrder', 'asc').get();
    const podcasts = [];
    snap.forEach(doc => podcasts.push({ id: doc.id, ...doc.data() }));
    res.json({ podcasts });
  } catch (e) {
    try {
      const db = admin.firestore();
      const snap = await db.collection('podcasts').get();
      const podcasts = [];
      snap.forEach(doc => podcasts.push({ id: doc.id, ...doc.data() }));
      res.json({ podcasts });
    } catch (e2) { res.status(500).json({ error: e2.message }); }
  }
});

app.post('/api/podcasts', async (req, res) => {
  try {
    const db = admin.firestore();
    const { title, feedUrl, description, artworkUrl, category, enabled, featured, sortOrder } = req.body;
    if (!title) return res.status(400).json({ error: 'title required' });
    if (!feedUrl) return res.status(400).json({ error: 'feedUrl required' });
    const data = {
      title,
      feedUrl,
      description: description || '',
      artworkUrl: artworkUrl || '',
      category: category || 'sermons',
      enabled: enabled !== false,
      featured: !!featured,
      sortOrder: Number(sortOrder) || 0,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    const ref = await db.collection('podcasts').add(data);
    res.json({ id: ref.id, ...data });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/podcasts/:id', async (req, res) => {
  try {
    const db = admin.firestore();
    const { title, feedUrl, description, artworkUrl, category, enabled, featured, sortOrder } = req.body;
    const update = {};
    if (title !== undefined) update.title = title;
    if (feedUrl !== undefined) update.feedUrl = feedUrl;
    if (description !== undefined) update.description = description;
    if (artworkUrl !== undefined) update.artworkUrl = artworkUrl;
    if (category !== undefined) update.category = category;
    if (enabled !== undefined) update.enabled = !!enabled;
    if (featured !== undefined) update.featured = !!featured;
    if (sortOrder !== undefined) update.sortOrder = Number(sortOrder);
    await db.collection('podcasts').doc(req.params.id).update(update);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/podcasts/:id', adminOnly, async (req, res) => {
  try {
    await admin.firestore().collection('podcasts').doc(req.params.id).delete();
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Fetch and parse podcast RSS feed
app.get('/api/podcasts/:id/episodes', async (req, res) => {
  try {
    const db = admin.firestore();
    const doc = await db.collection('podcasts').doc(req.params.id).get();
    if (!doc.exists) return res.status(404).json({ error: 'Podcast not found' });
    const { feedUrl } = doc.data();
    if (!feedUrl) return res.status(400).json({ error: 'No feed URL configured' });

    const feedRes = await fetch(feedUrl, {
      headers: { 'User-Agent': 'GO-Admin/1.0 (RSS Reader)' },
    });
    if (!feedRes.ok) throw new Error(`Feed returned ${feedRes.status}`);
    const xml = await feedRes.text();

    // Parse RSS with fast-xml-parser
    let XMLParser;
    try {
      ({ XMLParser } = require('fast-xml-parser'));
    } catch {
      return res.status(503).json({ error: 'RSS parser not installed (run npm install)' });
    }
    const parser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: '@_',
      isArray: (name) => name === 'item',
    });
    const feed = parser.parse(xml);
    const channel = feed?.rss?.channel || {};
    const items = (Array.isArray(channel.item) ? channel.item : channel.item ? [channel.item] : []).slice(0, 30);

    const episodes = items.map(item => ({
      title: item.title || '',
      description: item.description || item['itunes:summary'] || '',
      pubDate: item.pubDate || '',
      duration: item['itunes:duration'] || '',
      audioUrl: item.enclosure?.['@_url'] || '',
      guid: item.guid?.['#text'] || item.guid || '',
      image: item['itunes:image']?.['@_href'] || '',
    }));

    res.json({
      feedTitle: channel.title || '',
      feedDescription: channel.description || '',
      feedImage: channel.image?.url || channel['itunes:image']?.['@_href'] || '',
      episodes,
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Audio Assets ──────────────────────────────────────────────────────────────

app.get('/api/audio', async (req, res) => {
  try {
    const db = admin.firestore();
    const snap = await db.collection('audioAssets').orderBy('sortOrder', 'asc').get();
    const audio = [];
    snap.forEach(doc => audio.push({ id: doc.id, ...doc.data() }));
    res.json({ audio });
  } catch (e) {
    try {
      const db = admin.firestore();
      const snap = await db.collection('audioAssets').get();
      const audio = [];
      snap.forEach(doc => audio.push({ id: doc.id, ...doc.data() }));
      res.json({ audio });
    } catch (e2) { res.status(500).json({ error: e2.message }); }
  }
});

app.post('/api/audio', async (req, res) => {
  try {
    const db = admin.firestore();
    const { title, artist, description, audioUrl, coverImageUrl, category, duration, featured, sortOrder, seriesId, episodeNumber, mediaType } = req.body;
    if (!title) return res.status(400).json({ error: 'title required' });
    const data = {
      title,
      artist: artist || '',
      description: description || '',
      audioUrl: audioUrl || '',
      coverImageUrl: coverImageUrl || '',
      category: category || 'music',
      duration: Number(duration) || 0,
      featured: !!featured,
      sortOrder: Number(sortOrder) || 0,
      mediaType: mediaType || 'audio',
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    if (seriesId) data.seriesId = seriesId;
    if (episodeNumber !== undefined && episodeNumber !== null && episodeNumber !== '') data.episodeNumber = Number(episodeNumber);
    const ref = await db.collection('audioAssets').add(data);
    res.json({ id: ref.id, ...data });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/audio/:id', async (req, res) => {
  try {
    const db = admin.firestore();
    const { title, artist, description, audioUrl, coverImageUrl, category, duration, featured, sortOrder, seriesId, episodeNumber, mediaType } = req.body;
    const update = {};
    if (title !== undefined) update.title = title;
    if (artist !== undefined) update.artist = artist;
    if (description !== undefined) update.description = description;
    if (audioUrl !== undefined) update.audioUrl = audioUrl;
    if (coverImageUrl !== undefined) update.coverImageUrl = coverImageUrl;
    if (category !== undefined) update.category = category;
    if (duration !== undefined) update.duration = Number(duration);
    if (featured !== undefined) update.featured = !!featured;
    if (sortOrder !== undefined) update.sortOrder = Number(sortOrder);
    if (mediaType !== undefined) update.mediaType = mediaType;
    if (seriesId !== undefined) update.seriesId = seriesId || null;
    if (episodeNumber !== undefined) update.episodeNumber = (episodeNumber !== null && episodeNumber !== '') ? Number(episodeNumber) : null;
    await db.collection('audioAssets').doc(req.params.id).update(update);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/audio/:id', adminOnly, async (req, res) => {
  try {
    await admin.firestore().collection('audioAssets').doc(req.params.id).delete();
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Series ────────────────────────────────────────────────────────────────────

app.get('/api/series', async (req, res) => {
  try {
    const db = admin.firestore();
    const snap = await db.collection('series').orderBy('sortOrder', 'asc').get();
    const series = [];
    snap.forEach(doc => series.push({ id: doc.id, ...doc.data() }));
    res.json({ series });
  } catch (e) {
    try {
      const db = admin.firestore();
      const snap = await db.collection('series').get();
      const series = [];
      snap.forEach(doc => series.push({ id: doc.id, ...doc.data() }));
      res.json({ series });
    } catch (e2) { res.status(500).json({ error: e2.message }); }
  }
});

app.post('/api/series', async (req, res) => {
  try {
    const db = admin.firestore();
    const { title, description, artworkUrl, category, mediaType, enabled, featured, sortOrder } = req.body;
    if (!title) return res.status(400).json({ error: 'title required' });
    const data = {
      title,
      description: description || '',
      artworkUrl: artworkUrl || '',
      category: category || 'sermons',
      mediaType: mediaType || 'audio',
      enabled: enabled !== false,
      featured: !!featured,
      sortOrder: Number(sortOrder) || 0,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    const ref = await db.collection('series').add(data);
    res.json({ id: ref.id, ...data });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/series/:id', async (req, res) => {
  try {
    const db = admin.firestore();
    const { title, description, artworkUrl, category, mediaType, enabled, featured, sortOrder } = req.body;
    const update = {};
    if (title !== undefined) update.title = title;
    if (description !== undefined) update.description = description;
    if (artworkUrl !== undefined) update.artworkUrl = artworkUrl;
    if (category !== undefined) update.category = category;
    if (mediaType !== undefined) update.mediaType = mediaType;
    if (enabled !== undefined) update.enabled = !!enabled;
    if (featured !== undefined) update.featured = !!featured;
    if (sortOrder !== undefined) update.sortOrder = Number(sortOrder);
    await db.collection('series').doc(req.params.id).update(update);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/series/:id', adminOnly, async (req, res) => {
  try {
    await admin.firestore().collection('series').doc(req.params.id).delete();
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/series/:id/episodes', async (req, res) => {
  try {
    const db = admin.firestore();
    const snap = await db.collection('audioAssets')
      .where('seriesId', '==', req.params.id)
      .get();
    const episodes = [];
    snap.forEach(doc => episodes.push({ id: doc.id, ...doc.data() }));
    // Sort by episodeNumber ascending
    episodes.sort((a, b) => (a.episodeNumber || 0) - (b.episodeNumber || 0));
    res.json({ episodes });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Featured Videos Config ─────────────────────────────
app.get('/api/config/featured', async (req, res) => {
  try {
    const db = admin.firestore();
    const doc = await db.collection('config').doc('featured').get();
    res.json({ ids: doc.exists ? (doc.data().ids || []) : [] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/config/featured', async (req, res) => {
  try {
    const db = admin.firestore();
    const { ids } = req.body;
    if (!Array.isArray(ids)) return res.status(400).json({ error: 'ids must be an array' });
    await db.collection('config').doc('featured').set({ ids });
    res.json({ ok: true, count: ids.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.listen(PORT, async () => {
  console.log(`GO Admin running on :${PORT}`);
  if (sa) await loadPortalEditors();
});
// Railway redeploy trigger 1780454656
// deploy 1780466198
