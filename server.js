const express = require('express');
const basicAuth = require('express-basic-auth');
const multer = require('multer');
const sharp = require('sharp');
const admin = require('firebase-admin');

// ── Firebase Admin SDK ───────────────────────────────────
const sa = process.env.FIREBASE_SA ? JSON.parse(process.env.FIREBASE_SA) : null;
if (sa) {
  admin.initializeApp({ credential: admin.credential.cert(sa) });
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
// Thumbnail serving is public (the app needs to load these without credentials)
const requireAuth = basicAuth({ users: { [ADMIN_USER]: ADMIN_PASS }, challenge: true, realm: 'GO Admin' });
app.use((req, res, next) => {
  if (req.method === 'GET' && req.path.startsWith('/api/thumbnails/')) return next();
  if (req.path === '/webhooks/mux') return next(); // Mux webhooks bypass basic auth
  if (req.path === '/cast-receiver.html') return next(); // Cast receiver must be publicly accessible
  requireAuth(req, res, next);
});

app.use(express.json());

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
      'ripZz37iQh24znVPRqkrZR02N8Sl2Zs1021b2lyUwHHVQ': 'Sermon',
      'Dbckmh4c8WKzhY8ieBQ01jhBdV1LwJuSOTpkaDT3uKH00': 'Recitals',
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
      await mux('PATCH', `/video/v1/assets/${assetId}`, {
        meta: { title },
        passthrough: serializePassthrough(pt),
      });
      console.log(`[webhook] Auto-titled asset ${assetId}: "${title}"`);
    } catch (err) {
      console.error('[webhook] Failed to auto-title asset:', err.message);
    }
  }

  res.sendStatus(200);
});
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
      new_asset_settings: { playback_policy: ['public'] },
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

// DELETE /api/thumbnails/:assetId
app.delete('/api/thumbnails/:assetId', async (req, res) => {
  try {
    await fsDelete('thumbnails', req.params.assetId);
    res.json({ ok: true });
  } catch (e) {
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

app.delete('/api/feedback/:id', async (req, res) => {
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
    // Look up last platform from sessions collection
    const platformMap = {};
    try {
      const sessSnap = await admin.firestore().collection('sessions')
        .limit(5000).get();
      sessSnap.forEach(doc => {
        const d = doc.data();
        if (d.uid && d.platform) platformMap[d.uid] = d.platform;
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
      u.platform = platformMap[u.uid] || null;
      u.minutesWatched = watchMap[u.uid] || 0;
      u.privateAccess = !!privateMap[u.uid];
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
app.patch('/api/users/:uid/block', async (req, res) => {
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
app.delete('/api/users/:uid', async (req, res) => {
  if (!sa) return res.status(503).json({ error: 'Firebase Admin not configured' });
  try {
    await admin.auth().deleteUser(req.params.uid);
    await admin.firestore().collection('users').doc(req.params.uid).delete().catch(() => {});
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () => console.log(`GO Admin running on :${PORT}`));
