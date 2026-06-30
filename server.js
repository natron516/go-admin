const ADMIN_BUILD = 245;
const crypto = require('crypto');
const express = require('express');
const basicAuth = require('express-basic-auth');
const multer = require('multer');
const sharp = require('sharp');
const admin = require('firebase-admin');
const nodemailer = require('nodemailer');

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
// Reused from the (removed) scripture-detection feature; already set in Railway.
// Optional: when absent, word-level timing is simply skipped and the app falls
// back to the existing sentence-level cue sync. Never breaks the Mux path.
const DEEPGRAM_KEY  = process.env.DEEPGRAM_API_KEY || '';

// ── Outbound email (portal editor credentials) ───────────────────────────────
// Uses Gmail SMTP with an App Password. Set these in Railway:
//   SMTP_USER  = the Gmail address to send from (e.g. natesclaw16@gmail.com)
//   SMTP_PASS  = a Gmail App Password (NOT the account password)
//   EDITOR_FROM (optional) = display From, e.g. "GO Media <natesclaw16@gmail.com>"
// When SMTP creds are absent, email is skipped gracefully (the admin still gets
// the credentials in the browser to copy/share manually).
const SMTP_USER   = process.env.SMTP_USER || '';
const SMTP_PASS   = process.env.SMTP_PASS || '';
const EDITOR_FROM = process.env.EDITOR_FROM || (SMTP_USER ? `GO Media Admin <${SMTP_USER}>` : '');
const PORTAL_URL  = process.env.PORTAL_URL || 'https://go-admin-production-6be4.up.railway.app';

let mailTransport = null;
function getMailTransport() {
  if (!SMTP_USER || !SMTP_PASS) return null;
  if (!mailTransport) {
    mailTransport = nodemailer.createTransport({
      service: 'gmail',
      auth: { user: SMTP_USER, pass: SMTP_PASS },
    });
  }
  return mailTransport;
}

// Send portal-editor login credentials to a new editor. Returns
// { sent: true } on success, or { sent: false, reason } when skipped/failed.
async function sendEditorCredentialsEmail({ to, displayName, username, password }) {
  const tx = getMailTransport();
  if (!tx) return { sent: false, reason: 'email-not-configured' };
  if (!to) return { sent: false, reason: 'no-recipient' };
  const name = displayName || 'there';
  const text =
`Hi ${name},

You've been granted editor access to the GO Media admin portal.

Portal: ${PORTAL_URL}
Username: ${username}
Password: ${password}

You can sign in with the credentials above. For security, please keep them private.

— GO Media`;
  const html =
`<p>Hi ${name},</p>
<p>You've been granted <b>editor access</b> to the GO Media admin portal.</p>
<p>
<b>Portal:</b> <a href="${PORTAL_URL}">${PORTAL_URL}</a><br>
<b>Username:</b> ${username}<br>
<b>Password:</b> ${password}
</p>
<p>You can sign in with the credentials above. For security, please keep them private.</p>
<p>— GO Media</p>`;
  try {
    await tx.sendMail({
      from: EDITOR_FROM, to,
      subject: 'Your GO Media admin portal access',
      text, html,
    });
    return { sent: true };
  } catch (e) {
    console.error('[editor-email] send failed:', e.message);
    return { sent: false, reason: e.message };
  }
}

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
// Content management (incl. deleting content) is allowed for editors AND admins.
// Sensitive actions (users, portal-editors, feedback) stay adminOnly.
function editorOrAdmin(req, res, next) {
  if (req.userRole !== 'admin' && req.userRole !== 'editor') {
    return res.status(403).json({ error: 'Editor or admin access required' });
  }
  next();
}
app.use((req, res, next) => {
  // Public routes
  if (req.method === 'GET' && req.path.startsWith('/api/thumbnails/')) return next();
  // Deepgram WebVTT caption files must be fetchable by Mux (unauthenticated).
  if (req.method === 'GET' && req.path.startsWith('/api/deepgram-vtt/')) return next();
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
    // Best-effort: email the credentials to the new editor.
    const mail = await sendEditorCredentialsEmail({
      to: email, displayName: data.displayName, username: data.username, password,
    });
    res.json({ ok: true, username: data.username, password, role: 'editor',
               emailSent: mail.sent, emailReason: mail.reason || null });
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

  // Auto-transcribe every asset when it becomes ready (live recordings AND uploads)
  if (event.type === 'video.asset.ready') {
    try {
      await autoTranscribe(event.data);
    } catch (err) {
      console.error('[webhook] Auto-transcribe failed:', err.message);
    }
    // Deepgram needs a static MP4/audio rendition to read. For sermons, make
    // sure mp4_support is enabled so renditions generate; the Deepgram word
    // generation then runs on the video.asset.static_renditions.ready event
    // below (renditions aren't ready at asset.ready time for long recordings,
    // which caused 404s + missing transcripts). Non-sermon spoken word still
    // tries words now (uploads usually already have a usable rendition).
    // Enable mp4_support (standard) for EVERY non-music asset so a static
    // audio.m4a rendition exists for Deepgram word-timing generation. Was
    // sermon-only, which left audiobooks/other spoken word without follow-along
    // word-sync. (Isaac, 6/29: transcript + timings by default for all non-music.)
    try {
      if (shouldAutoTranscribe(event.data) && event.data?.mp4_support !== 'standard') {
        await mux('PUT', `/video/v1/assets/${event.data.id}/mp4-support`, { mp4_support: 'standard' });
        console.log(`[webhook] Enabled mp4_support for ${event.data.id} (asset.ready, non-music)`);
      }
    } catch (e) {
      console.error(`[webhook] mp4_support enable failed: ${e.message}`);
    }
    // ADDITIVE: also generate Deepgram word-level timings (fire-and-forget,
    // fully guarded). For sermons this often no-ops here (rendition not ready)
    // and succeeds on static_renditions.ready instead.
    maybeGenerateWords(event.data);
  }

  // Static renditions (MP4/audio) finished generating — NOW Deepgram can read
  // the audio. This is the reliable trigger for sermon word-level transcripts
  // on long live recordings (asset.ready fires before renditions exist).
  if (event.type === 'video.asset.static_renditions.ready' ||
      event.type === 'video.asset.static_renditions.preparing') {
    if (event.type === 'video.asset.static_renditions.ready') {
      try {
        // event.data is the asset; ensure we have full asset (with playback id).
        const assetId = event.data?.id || event.object?.id;
        if (assetId) {
          const full = (await mux('GET', `/video/v1/assets/${assetId}`)).data;
          if (full) maybeGenerateWords(full);
        }
      } catch (err) {
        console.error('[webhook] static_renditions.ready words failed:', err.message);
      }
    }
  }

  // When a sermon live stream goes IDLE (after Sunday's broadcast ends), make
  // sure auto-generated LIVE captions are configured for the NEXT broadcast.
  // Mux only allows configuring generated_subtitles while the stream is idle, so
  // this is the reliable moment to set it. Safe + idempotent.
  if (event.type === 'video.live_stream.idle') {
    try {
      const sid = event.data?.id;
      if (sid && SERMON_STREAM_IDS.has(sid)) await ensureLiveCaptions(sid);
    } catch (err) {
      console.error('[webhook] ensureLiveCaptions failed:', err.message);
    }
  }

  res.sendStatus(200);
});

// Live stream IDs whose recordings are sermons (auto-transcription scope)
const SERMON_STREAM_IDS = new Set([
  'ECgSydhoD601OoMqVmiNvfH6y0100m8uENxk6KKJV4NSZQ',
]);

// Enable Mux auto-generated LIVE closed captions (English) on a live stream.
// Mux requires the stream to be IDLE to (re)configure generated_subtitles; if
// the stream is active this throws and the caller should retry once it's idle.
// Idempotent: if an "en" generated subtitle is already configured, it's a no-op.
async function ensureLiveCaptions(streamId) {
  const cur = await mux('GET', `/video/v1/live-streams/${streamId}`);
  const stream = cur.data;
  if (!stream) throw new Error('stream not found');
  const existing = stream.generated_subtitles || [];
  if (existing.some(g => (g.language_code || 'en').toLowerCase().startsWith('en'))) {
    console.log(`[live-captions] ${streamId} already has English generated captions — skip`);
    return { ok: true, skipped: true };
  }
  if (stream.status === 'active') {
    throw new Error('stream is active; live captions can only be configured while idle');
  }
  await mux('PUT', `/video/v1/live-streams/${streamId}/generated-subtitles`, {
    generated_subtitles: [{ name: 'English (auto)', language_code: 'en' }],
  });
  console.log(`[live-captions] Enabled English generated live captions on ${streamId}`);
  return { ok: true, enabled: true };
}

function isSermonAsset(asset) {
  if (asset.live_stream_id && SERMON_STREAM_IDS.has(asset.live_stream_id)) return true;
  const pt = parsePassthrough(asset.passthrough);
  return (pt.category || '').toLowerCase().trim() === 'sermon';
}

// Categories that are NOT spoken word — transcription/highlight is skipped.
const NON_SPOKEN_CATEGORIES = new Set(['music']);

// Default policy (Isaac, 2026-06-19): transcribe + word-highlight EVERY uploaded
// asset that carries spoken word, i.e. anything that isn't music. Sermon live
// recordings always qualify. Music is excluded. Assets with no audio track are
// skipped downstream where the audio track is required.
function shouldAutoTranscribe(asset) {
  if (asset.live_stream_id && SERMON_STREAM_IDS.has(asset.live_stream_id)) return true;
  const pt = parsePassthrough(asset.passthrough);
  const cat = (pt.category || '').toLowerCase().trim();
  return !NON_SPOKEN_CATEGORIES.has(cat);
}

// Kick off Mux subtitle generation for any spoken-word asset (non-music) if it
// has audio and no text track yet.
async function autoTranscribe(asset) {
  const assetId = asset.id;
  if (!shouldAutoTranscribe(asset)) return; // spoken word only (skip music)
  const tracks = asset.tracks || [];
  if (tracks.some(t => t.type === 'text')) return; // already has/generating captions
  const audioTrack = tracks.find(t => t.type === 'audio');
  if (!audioTrack) return;
  await mux('POST', `/video/v1/assets/${assetId}/tracks/${audioTrack.id}/generate-subtitles`, {
    generated_subtitles: [{ language_code: 'en', name: 'English (generated)' }],
  });
  console.log(`[transcript] Auto-started transcription for ${assetId}`);
}

// ── Deepgram word-level timing (ADDITIVE) ──────────────────────────────────────
// Stores per-word start/end timestamps in a NEW Firestore collection
// `transcriptWords/{playbackId}` so the app can highlight the exact spoken word
// and seek into the middle of a track accurately. This is COMPLETELY separate
// from the existing Mux auto-caption (VTT) path used by /api/audio-transcript/*.
// If anything here fails or the key is missing, the app falls back to the
// existing sentence-level cue sync — nothing is overwritten or removed.
//
// Firestore doc shape (transcriptWords/{playbackId}):
//   { words: [{ w: "Hello", s: 12.34, e: 12.61 }, ...], assetId, model, updatedAt }

// Call Deepgram's pre-recorded REST API with a remote audio URL. Deepgram
// CANNOT decode the Mux HLS playlist (.m3u8) — it returns "corrupt or
// unsupported data". Use the static MP4/M4A rendition instead
// (https://stream.mux.com/<pid>/audio.m4a), which requires mp4_support:standard
// (already enabled on ingest). No ffmpeg needed — Deepgram fetches/decodes it.
async function deepgramWordsForPlayback(playbackId) {
  if (!DEEPGRAM_KEY) throw new Error('DEEPGRAM_API_KEY not set');
  // Prefer the audio-only m4a rendition; if it isn't generated (Mux only made
  // video MP4 static renditions), fall back to the smallest video MP4 — Deepgram
  // extracts the audio track from it just fine.
  const candidates = [
    `https://stream.mux.com/${playbackId}/audio.m4a`,
    `https://stream.mux.com/${playbackId}/low.mp4`,
    `https://stream.mux.com/${playbackId}/medium.mp4`,
  ];
  let audioUrl = candidates[0];
  for (const url of candidates) {
    try {
      const head = await fetch(url, { method: 'HEAD' });
      if (head.ok) { audioUrl = url; break; }
    } catch { /* try next */ }
  }
  const params = new URLSearchParams({
    model: 'nova-2', language: 'en', smart_format: 'true', punctuate: 'true',
  });
  const r = await fetch(`https://api.deepgram.com/v1/listen?${params}`, {
    method: 'POST',
    headers: { Authorization: `Token ${DEEPGRAM_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ url: audioUrl }),
  });
  if (!r.ok) {
    const t = await r.text().catch(() => '');
    throw new Error(`Deepgram ${r.status} (src ${audioUrl.split('/').pop()}): ${t.slice(0, 200)}`);
  }
  const json = await r.json();
  const alt = json?.results?.channels?.[0]?.alternatives?.[0];
  const rawWords = alt?.words || [];
  // Keep payload small: punctuated_word for display, start/end rounded to ms.
  const words = rawWords.map(w => ({
    w: w.punctuated_word || w.word || '',
    s: Math.round((w.start ?? 0) * 1000) / 1000,
    e: Math.round((w.end ?? 0) * 1000) / 1000,
  })).filter(w => w.w);
  return words;
}

// Generate + persist word timings for one playback id (idempotent unless force).
// Returns { ok, count, skipped? }. Never throws into callers that wrap it.
async function generateAndStoreWords(playbackId, assetId, { force = false } = {}) {
  if (!DEEPGRAM_KEY) return { ok: false, error: 'DEEPGRAM_API_KEY not set' };
  const db = admin.firestore();
  const ref = db.collection('transcriptWords').doc(playbackId);
  if (!force) {
    const existing = await ref.get();
    if (existing.exists && Array.isArray(existing.data()?.words) && existing.data().words.length) {
      return { ok: true, count: existing.data().words.length, skipped: true };
    }
  }
  const words = await deepgramWordsForPlayback(playbackId);
  if (!words.length) return { ok: false, error: 'no words returned' };
  await ref.set({
    words,
    assetId: assetId || null,
    model: 'nova-2',
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  }, { merge: true });
  return { ok: true, count: words.length };
}

// Fire-and-forget word-timing generation for a freshly-ready sermon asset.
// Fully guarded: any failure is logged and swallowed so the existing Mux
// caption path is never affected. Mux captions are NOT required to be ready
// for this (Deepgram transcribes the audio directly).
async function maybeGenerateWords(asset) {
  try {
    if (!DEEPGRAM_KEY) return;
    if (!shouldAutoTranscribe(asset)) return; // spoken word only (skip music)
    const pid = asset.playback_ids?.[0]?.id;
    if (!pid) return;
    const res = await generateAndStoreWords(pid, asset.id);
    if (res.ok) console.log(`[words] Stored ${res.count} word timings for ${asset.id} (pid ${pid})${res.skipped ? ' [skipped, existed]' : ''}`);
    else console.warn(`[words] Skipped word timings for ${asset.id}: ${res.error}`);
    // SERMONS: replace Mux's auto-captions (which mislabel speech as "[Music]")
    // with a Deepgram-built caption track by default once words exist. (Isaac,
    // 6/29) Guarded + fire-and-forget; non-sermon assets keep Mux captions.
    if (res.ok && isSermonAsset(asset)) {
      try { await swapToDeepgramCaptions(pid, asset.id); }
      catch (e) { console.warn(`[deepgram-cc] swap failed for ${pid}: ${e.message}`); }
    }
  } catch (e) {
    console.error(`[words] Word-timing generation failed for ${asset?.id}: ${e.message}`);
  }
}

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
  const text = await res.text();
  if (!text) return {}; // e.g. 204 No Content on DELETE
  try { return JSON.parse(text); } catch { return { raw: text }; }
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

// Server-side cache of the full Mux asset list. Paginating the Mux API (~220
// assets / 3 pages) is the slowest part of a cold app launch (~8s), so we cache
// it in RAM and serve stale-while-revalidate: a request past the TTL gets the
// last good copy INSTANTLY while a single background refresh runs. A periodic
// timer also keeps the cache warm so the very first user of the hour isn't slow.
let _assetsCache = { data: null, at: 0 };
let _assetsRefreshing = null; // in-flight refresh promise (dedupes concurrent rebuilds)
const ASSETS_CACHE_MS = 5 * 60 * 1000;       // serve from cache without refresh
const ASSETS_STALE_MS = 30 * 60 * 1000;      // hard limit: must block & rebuild past this

async function _rebuildMuxAssets() {
  let allAssets = [];
  let cursor = null;
  while (true) {
    const data = await mux('GET', cursor
      ? `/video/v1/assets?limit=100&order_direction=desc&cursor=${encodeURIComponent(cursor)}`
      : '/video/v1/assets?limit=100&order_direction=desc');
    if (data.data) allAssets.push(...data.data);
    if (data.next_cursor) { cursor = data.next_cursor; } else { break; }
  }
  // Augment with server-tracked MP4 preparation state.
  const t = Date.now();
  for (const a of allAssets) {
    if (_mp4Preparing[a.id]) {
      if (t - _mp4Preparing[a.id].startedAt > 10 * 60 * 1000) delete _mp4Preparing[a.id];
      else a._mp4Preparing = true;
    }
  }
  _assetsCache = { data: allAssets, at: Date.now() };
  return allAssets;
}

function _refreshMuxAssetsInBackground() {
  if (_assetsRefreshing) return _assetsRefreshing;
  _assetsRefreshing = _rebuildMuxAssets()
    .catch(e => { console.warn('Mux asset refresh failed:', e.message); return _assetsCache.data; })
    .finally(() => { _assetsRefreshing = null; });
  return _assetsRefreshing;
}

async function fetchAllMuxAssets() {
  const now = Date.now();
  const age = now - _assetsCache.at;
  if (_assetsCache.data) {
    // Fresh: serve as-is.
    if (age < ASSETS_CACHE_MS) return _assetsCache.data;
    // Stale-but-usable: serve instantly, refresh in background.
    if (age < ASSETS_STALE_MS) { _refreshMuxAssetsInBackground(); return _assetsCache.data; }
  }
  // No cache or too stale: must wait for a (deduped) rebuild.
  return _refreshMuxAssetsInBackground();
}

// Keep the cache warm so no user pays the cold-pagination cost.
setInterval(() => { _refreshMuxAssetsInBackground(); }, ASSETS_CACHE_MS).unref?.();

// Helper: read a Firestore collection ordered by sortOrder (falls back to
// unordered if the composite index is missing). Returns [] on error so one slow
// or failing collection can't break the aggregated /api/home response.
async function readCollection(name) {
  const db = admin.firestore();
  try {
    const snap = await db.collection(name).orderBy('sortOrder', 'asc').get();
    const out = [];
    snap.forEach(d => out.push({ id: d.id, ...d.data() }));
    return out;
  } catch (e) {
    try {
      const snap = await db.collection(name).get();
      const out = [];
      snap.forEach(d => out.push({ id: d.id, ...d.data() }));
      return out;
    } catch { return []; }
  }
}

// Aggregated home payload: ONE request returns everything the home/featured
// screen needs (videos + audio/series/podcasts/books/articles + live stream),
// instead of the app firing ~8 separate calls and paginating Mux client-side.
// Mux assets are served from a 60s server cache, so this is fast and cheap.
app.get('/api/home', async (req, res) => {
  try {
    const [assets, audio, series, podcasts, books, articles, live] = await Promise.all([
      fetchAllMuxAssets().catch(() => []),
      readCollection('audioAssets'),
      readCollection('series'),
      readCollection('podcasts'),
      readCollection('books'),
      readCollection('articles'),
      mux('GET', '/video/v1/live-streams').then(r => r.data || []).catch(() => []),
    ]);
    res.set('Cache-Control', 'public, max-age=60, stale-while-revalidate=300');
    res.json({ assets, audio, series, podcasts, books, articles, liveStreams: live });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/assets', async (req, res) => {
  try {
    const allAssets = await fetchAllMuxAssets();
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
    // Fetch current asset so we can preserve existing passthrough fields + title
    const current = await mux('GET', `/video/v1/assets/${req.params.id}`);
    const existing = parsePassthrough(current.data?.passthrough);
    existing.category = category || '';
    // Never clobber a real title with 'Untitled'. Only update meta.title when a
    // non-empty title is explicitly provided; otherwise keep the current one.
    const curTitle = current.data?.meta?.title;
    const newTitle = (title && title.trim() && title.trim() !== 'Untitled') ? title.trim() : (curTitle || '');
    const data = await mux('PATCH', `/video/v1/assets/${req.params.id}`, {
      meta: { title: newTitle },
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

// Set or clear the viewer START OFFSET (seconds) for an asset. Stored in
// passthrough so the app begins playback past the pre-stream graphic WITHOUT
// creating a new clip. PATCH /api/assets/:id/start-offset  body: { seconds }
// (null/0/empty clears it). Used by the admin portal for manual adjustment.
app.patch('/api/assets/:id/start-offset', async (req, res) => {
  try {
    const raw = req.body?.seconds;
    const current = await mux('GET', `/video/v1/assets/${req.params.id}`);
    const pt = parsePassthrough(current.data?.passthrough);
    const secs = Math.max(0, Math.round(Number(raw) || 0));
    if (secs > 0) pt.startOffset = String(secs);
    else delete pt.startOffset;
    const data = await mux('PATCH', `/video/v1/assets/${req.params.id}`, {
      passthrough: serializePassthrough(pt),
    });
    res.json({ ok: true, startOffset: secs, asset: data.data });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Auto-set the start offset on the latest Sunday sermon recording so playback
// begins when the pre-stream graphic comes down (~10:00 AM Pacific), minus a
// safety margin. Time-anchored: offset = (10:00 AM Pacific - stream start) -
// margin, clamped to >= 0. No new clip is created. Runs from the Sunday 1 PM
// cron. POST /api/sermons/set-sunday-start-offset  body: { sinceHours?, marginSeconds?, targetHour?, targetMinute? }
app.post('/api/sermons/set-sunday-start-offset', adminOnly, async (req, res) => {
  const sinceHours = Number(req.body?.sinceHours) || 12;
  const marginSeconds = req.body?.marginSeconds != null ? Math.max(0, Number(req.body.marginSeconds)) : 30;
  const targetHour = req.body?.targetHour != null ? Number(req.body.targetHour) : 10;
  const targetMinute = req.body?.targetMinute != null ? Number(req.body.targetMinute) : 0;
  const cutoff = Date.now() / 1000 - sinceHours * 3600;
  try {
    const data = await mux('GET', '/video/v1/assets?limit=20&order_direction=desc');
    const sermons = (data.data || [])
      .filter(a => isSermonAsset(a) && Number(a.created_at) >= cutoff)
      .sort((x, y) => Number(y.created_at) - Number(x.created_at));
    if (!sermons.length) return res.json({ status: 'none', message: `No sermon asset in the last ${sinceHours}h.` });
    const asset = sermons[0];

    // Compute where targetHour:targetMinute Pacific falls in the recording.
    const startMs = Number(asset.created_at) * 1000;
    const startPacific = new Date(new Date(startMs).toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }));
    const targetPacific = new Date(startPacific);
    targetPacific.setHours(targetHour, targetMinute, 0, 0);
    let offsetSeconds = Math.round((targetPacific - startPacific) / 1000) - marginSeconds;
    offsetSeconds = Math.max(0, offsetSeconds);
    const dur = Number(asset.duration) || Infinity;
    if (offsetSeconds >= dur) {
      return res.json({ status: 'skipped', assetId: asset.id, message: `Computed offset ${offsetSeconds}s >= duration; not set.` });
    }

    const pt = parsePassthrough(asset.passthrough);
    pt.startOffset = String(offsetSeconds);
    await mux('PATCH', `/video/v1/assets/${asset.id}`, { passthrough: serializePassthrough(pt) });
    // Bust the cached asset list so the app/portal see the new offset.
    _assetsCache = { data: null, at: 0 };
    console.log(`[sunday-start-offset] ${asset.id}: startOffset=${offsetSeconds}s (target ${targetHour}:${String(targetMinute).padStart(2,'0')} PT - ${marginSeconds}s margin)`);
    res.json({ status: 'set', assetId: asset.id, playbackId: asset.playback_ids?.[0]?.id || null, startOffset: offsetSeconds });
  } catch (e) {
    res.status(500).json({ status: 'error', error: e.message });
  }
});

// ── Transcripts ───────────────────────────────────────────────────────────
// Get or generate a transcript for an asset using Mux auto-generated captions.
// POST returns { status: 'ready', text } | { status: 'preparing' } | { status: 'errored'|'unavailable', message }
app.post('/api/assets/:id/transcript', async (req, res) => {
  try {
    const current = await mux('GET', `/video/v1/assets/${req.params.id}`);
    const asset = current.data;
    if (!asset) return res.status(404).json({ error: 'Asset not found' });

    const pid = asset.playback_ids?.[0]?.id;
    if (!pid) return res.status(400).json({ error: 'No playback ID' });

    // A custom/edited transcript (admin portal) wins over Mux/Deepgram entirely.
    {
      const ov = await getTranscriptOverride(pid);
      if (ov && ov.source === 'custom' && (ov.customText || '').trim()) {
        return res.json({ status: 'ready', text: ov.customText, source: 'custom' });
      }
    }

    const tracks = asset.tracks || [];
    const textTrack = tracks.find(t => t.type === 'text' && t.text_type === 'subtitles');

    // No captions yet — kick off Mux auto-caption generation on the audio track
    if (!textTrack) {
      const audioTrack = tracks.find(t => t.type === 'audio');
      if (!audioTrack) return res.json({ status: 'unavailable', message: 'No audio track on this asset.' });
      try {
        await mux('POST', `/video/v1/assets/${req.params.id}/tracks/${audioTrack.id}/generate-subtitles`, {
          generated_subtitles: [{ language_code: 'en', name: 'English (generated)' }],
        });
        console.log(`[transcript] Started caption generation for ${req.params.id}`);
        return res.json({ status: 'preparing', message: 'Transcription started — this takes a few minutes for long videos.' });
      } catch (e) {
        console.error(`[transcript] generate-subtitles failed: ${e.message}`);
        return res.json({ status: 'errored', message: 'Could not start transcription: ' + e.message });
      }
    }

    if (textTrack.status === 'preparing') {
      return res.json({ status: 'preparing', message: 'Transcription in progress…' });
    }
    if (textTrack.status === 'errored') {
      return res.json({ status: 'errored', message: 'Transcription failed for this asset.' });
    }

    // Ready — build cleaned transcript (Deepgram override if set, else Mux VTT)
    const clean = await buildBestTranscript(asset);
    if (!clean) return res.json({ status: 'errored', message: 'Transcript file not retrievable yet — try again shortly.' });
    const note = clean.sermonStart > 0 ? `(Pre-sermon music/singing removed — sermon begins ~${fmtTimestamp(clean.sermonStart)} in the recording)\n\n` : '';
    const text = (await buildRefsHeader(asset)) + note + clean.text;
    res.json({ status: 'ready', text, trackId: textTrack.id });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Resolve a Mux PLAYBACK ID -> asset ID. Audio tracks in the app only carry the
// stream URL (https://stream.mux.com/<playbackId>.m3u8), so the app passes the
// playback ID and we map it to the underlying asset.
async function assetIdForPlaybackId(playbackId) {
  const data = await mux('GET', `/video/v1/playback-ids/${playbackId}`);
  return data?.data?.object?.id || null; // { object: { type: 'asset', id } }
}

// Get or generate a transcript for an AUDIO track by its Mux playback ID.
// Used by the iOS/tvOS app: tapping the transcript icon on an audio track hits
// this with the playback ID parsed from the track's stream URL.
// GET  -> returns transcript if ready, else current status (does NOT start it)
// POST -> returns transcript if ready, else kicks off caption generation
app.all('/api/audio-transcript/:playbackId', async (req, res) => {
  try {
    const assetId = await assetIdForPlaybackId(req.params.playbackId);
    if (!assetId) return res.status(404).json({ status: 'unavailable', message: 'No asset for that playback ID.' });
    const current = await mux('GET', `/video/v1/assets/${assetId}`);
    const asset = current.data;
    if (!asset) return res.status(404).json({ status: 'unavailable', message: 'Asset not found.' });

    // A custom/edited transcript (admin portal) wins over Mux/Deepgram entirely.
    {
      const ov = await getTranscriptOverride(req.params.playbackId);
      if (ov && ov.source === 'custom' && (ov.customText || '').trim()) {
        return res.json({ status: 'ready', text: ov.customText, source: 'custom' });
      }
    }

    const tracks = asset.tracks || [];
    const textTrack = tracks.find(t => t.type === 'text' && t.text_type === 'subtitles');

    // Sermons serve from Deepgram by default, so a usable transcript can be
    // returned even if the Mux caption track is missing/preparing/errored.
    const tryDeepgram = async () => {
      if (!isSermonAsset(asset)) return null;
      const built = await buildBestTranscript(asset);
      if (!built) return null;
      const note = built.sermonStart > 0 ? `(Pre-sermon music/singing removed — sermon begins ~${fmtTimestamp(built.sermonStart)} in the recording)\n\n` : '';
      return { status: 'ready', text: (await buildRefsHeader(asset)) + note + built.text, trackId: textTrack?.id || null };
    };

    if (!textTrack) {
      const dg = await tryDeepgram();
      if (dg) return res.json(dg);
      if (req.method !== 'POST') return res.json({ status: 'none', message: 'No transcript yet.' });
      const audioTrack = tracks.find(t => t.type === 'audio');
      if (!audioTrack) return res.json({ status: 'unavailable', message: 'No audio track on this asset.' });
      try {
        await mux('POST', `/video/v1/assets/${assetId}/tracks/${audioTrack.id}/generate-subtitles`, {
          generated_subtitles: [{ language_code: 'en', name: 'English (generated)' }],
        });
        console.log(`[audio-transcript] Started caption generation for asset ${assetId} (pid ${req.params.playbackId})`);
        return res.json({ status: 'preparing', message: 'Transcription started — takes a few minutes.' });
      } catch (e) {
        return res.json({ status: 'errored', message: 'Could not start transcription: ' + e.message });
      }
    }
    if (textTrack.status === 'preparing') {
      const dg = await tryDeepgram();
      if (dg) return res.json(dg);
      return res.json({ status: 'preparing', message: 'Transcription in progress…' });
    }
    if (textTrack.status === 'errored') {
      const dg = await tryDeepgram();
      if (dg) return res.json(dg);
      return res.json({ status: 'errored', message: 'Transcription failed.' });
    }

    const clean = await buildBestTranscript(asset);
    if (!clean) return res.json({ status: 'errored', message: 'Transcript file not retrievable yet — try again shortly.' });
    // Lectures have no worship intro; buildCleanTranscript only trims when it
    // detects pre-sermon singing, so for these tracks sermonStart is ~0 and the
    // full lecture text is returned. Prepend the scripture-reference header so
    // refs sit at the top (same format as the video-asset transcript path).
    const note = clean.sermonStart > 0 ? `(Pre-sermon music/singing removed — sermon begins ~${fmtTimestamp(clean.sermonStart)} in the recording)\n\n` : '';
    const text = (await buildRefsHeader(asset)) + note + clean.text;
    res.json({ status: 'ready', text, trackId: textTrack.id });
  } catch (e) {
    res.status(500).json({ status: 'errored', error: e.message });
  }
});

// Timed sentence CUES for live transcript sync ("karaoke" highlight + auto-scroll).
// Returns the SAME kept/cleaned cues that flow into the cleaned-text blob from
// GET /api/audio-transcript/:playbackId, but each carries its audio start/end
// time so the app can highlight the currently-spoken sentence and scroll to it.
//
// GET /api/audio-transcript/:playbackId/cues
// -> { status: "ready", cues: [{ start, end, text }], sermonStart }
//    | { status: "preparing"|"none"|"errored", message }
//
// Notes:
//  - cues are in playback order; `start`/`end` are seconds from the start of
//    the recording (NOT relative to the trimmed sermon).
//  - end = next kept cue's start; the final cue gets a +6s tail.
//  - the concatenated cue text (word order) matches the cleaned blob, so the
//    app can locate each cue inside the rendered transcript by word sequence.
app.get('/api/audio-transcript/:playbackId/cues', async (req, res) => {
  try {
    const assetId = await assetIdForPlaybackId(req.params.playbackId);
    if (!assetId) return res.status(404).json({ status: 'none', message: 'No asset for that playback ID.' });
    const current = await mux('GET', `/video/v1/assets/${assetId}`);
    const asset = current.data;
    if (!asset) return res.status(404).json({ status: 'none', message: 'Asset not found.' });
    const pid = asset.playback_ids?.[0]?.id;
    const textTrack = (asset.tracks || []).find(t => t.type === 'text' && t.text_type === 'subtitles');
    if (!textTrack) return res.json({ status: 'none', message: 'No transcript yet.' });
    if (textTrack.status === 'preparing') return res.json({ status: 'preparing', message: 'Transcription in progress…' });
    if (textTrack.status === 'errored') return res.json({ status: 'errored', message: 'Transcription failed.' });
    if (!pid || textTrack.status !== 'ready') return res.json({ status: 'preparing', message: 'Transcript not ready yet.' });

    const r = await fetch(`https://stream.mux.com/${pid}/text/${textTrack.id}.vtt`);
    if (!r.ok) return res.json({ status: 'errored', message: 'VTT fetch failed.' });
    const { kept, sermonStart } = trimCues(parseVtt(await r.text()));
    if (!kept.length) return res.json({ status: 'none', message: 'Empty transcript.' });

    // end = next cue's start; last cue gets a short tail so it stays "active".
    const cues = kept.map((c, i) => ({
      start: Math.round(c.start * 1000) / 1000,
      end: Math.round((kept[i + 1] ? kept[i + 1].start : c.start + 6) * 1000) / 1000,
      text: c.text,
    }));
    res.json({ status: 'ready', cues, sermonStart });
  } catch (e) {
    res.status(500).json({ status: 'errored', error: e.message });
  }
});

// ── Cross-sermon transcript SEARCH ─────────────────────────────────────────
// Isaac (2026-06-29): a search box at the top of the Sermons list that searches
// EVERY sermon's transcript for a word and returns each hit with the sermon, a
// timestamp, and the full sentence it was said in (tap -> play that moment).
//
// Implementation: build sentence-level cues per sermon (same source as the
// per-sermon /cues endpoint) and scan them. Cues are cached per playbackId so
// repeat searches don't re-fetch every VTT from Mux.
const _sermonCuesCache = new Map(); // pid -> { cues:[{start,end,text}], at }
const SERMON_CUES_TTL = 60 * 60 * 1000; // 1h; sermon transcripts don't change

// Build SHORT caption cues (for an on-video WebVTT) from Deepgram words: ~7-word
// chunks broken on punctuation so lines fit the screen + advance naturally.
// (Isaac, 6/29 — replace Mux's mislabeled "[Music]" captions with Deepgram.)
async function deepgramCaptionCues(pid, maxWords = 7) {
  try {
    const snap = await admin.firestore().collection('transcriptWords').doc(pid).get();
    const words = snap.exists ? snap.data()?.words : null;
    if (!Array.isArray(words) || !words.length) return [];
    const cues = [];
    let buf = [], startT = null, lastE = null;
    const flush = () => {
      if (!buf.length) return;
      const text = buf.join(' ').replace(/\s+([.,;:!?])/g, '$1').trim();
      if (text) cues.push({ start: startT ?? 0, end: (lastE ?? startT ?? 0), text });
      buf = []; startT = null;
    };
    for (const w of words) {
      const tok = w.w; if (!tok) continue;
      if (startT == null) startT = w.s ?? 0;
      buf.push(tok);
      lastE = w.e ?? w.s ?? startT;
      if (/[.!?]$/.test(tok) || buf.length >= maxWords) flush();
    }
    flush();
    return cues;
  } catch { return []; }
}

function vttTimestamp(sec) {
  const s = Math.max(0, sec || 0);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = Math.floor(s % 60);
  const ms = Math.round((s - Math.floor(s)) * 1000);
  const p = (n, l = 2) => String(n).padStart(l, '0');
  return `${p(h)}:${p(m)}:${p(ss)}.${p(ms, 3)}`;
}

// Public WebVTT built from Deepgram words. Mux ingests this URL as a caption
// track (see /api/sermons/use-deepgram-captions). Public on purpose so Mux can
// fetch it; contains only transcript text. (Isaac, 6/29)
app.get('/api/deepgram-vtt/:pid.vtt', async (req, res) => {
  try {
    const cues = await deepgramCaptionCues(req.params.pid);
    res.set('Content-Type', 'text/vtt; charset=utf-8');
    if (!cues.length) return res.send('WEBVTT\n\n');
    let out = 'WEBVTT\n\n';
    for (let i = 0; i < cues.length; i++) {
      const c = cues[i];
      const end = c.end > c.start ? c.end : c.start + 2;
      out += `${i + 1}\n${vttTimestamp(c.start)} --> ${vttTimestamp(end)}\n${c.text}\n\n`;
    }
    res.send(out);
  } catch (e) {
    res.status(500).send('WEBVTT\n\n');
  }
});

// Replace an asset's mislabeled Mux auto-captions with a Deepgram-built caption
// track: delete existing text track(s), then add a text track sourced from our
// public Deepgram VTT URL. The on-video CC then shows real speech instead of
// "[Music]". (Isaac, 6/29)
// Shared helper: swap an asset's Mux caption track(s) for a Deepgram-built one.
async function swapToDeepgramCaptions(pid, assetId) {
  if (!assetId) assetId = await assetIdForPlaybackId(pid);
  if (!assetId) throw new Error('No asset for that playback id');
  const cues = await deepgramCaptionCues(pid);
  if (!cues.length) throw new Error('No Deepgram words for this asset yet');
  const asset = (await mux('GET', `/video/v1/assets/${assetId}`)).data;
  const tracks = asset?.tracks || [];
  // If a Deepgram caption track is already present (our name 'English' + our
  // VTT), skip to keep this idempotent.
  let deleted = 0;
  for (const t of tracks.filter(t => t.type === 'text' && t.text_type === 'subtitles')) {
    try { await mux('DELETE', `/video/v1/assets/${assetId}/tracks/${t.id}`); deleted++; } catch (e) {}
  }
  const base = (process.env.PUBLIC_BASE_URL || 'https://go-admin-production-6be4.up.railway.app').replace(/\/$/, '');
  const vttUrl = `${base}/api/deepgram-vtt/${pid}.vtt`;
  const added = await mux('POST', `/video/v1/assets/${assetId}/tracks`, {
    url: vttUrl, type: 'text', text_type: 'subtitles',
    language_code: 'en', name: 'English', closed_captions: true,
  });
  _sermonCuesCache.delete(pid);
  console.log(`[deepgram-cc] ${pid}: deleted ${deleted} old track(s), added Deepgram VTT (${cues.length} cues)`);
  return { assetId, deletedTracks: deleted, cues: cues.length, vttUrl, track: added?.data?.id || null };
}

//   POST /api/sermons/use-deepgram-captions  body: { playbackId }
app.post('/api/sermons/use-deepgram-captions', adminOnly, async (req, res) => {
  const pid = req.body?.playbackId;
  if (!pid) return res.status(400).json({ error: 'playbackId required' });
  try {
    const r = await swapToDeepgramCaptions(pid, null);
    res.json({ ok: true, playbackId: pid, ...r,
      note: 'Deepgram caption track ingesting (~minutes). On-video CC will show real speech once ready.' });
  } catch (e) {
    res.status(422).json({ error: e.message });
  }
});

// Build timed SENTENCE cues from stored Deepgram words (transcriptWords/{pid}).
// Used as the search source when the Mux VTT is sparse/mislabeled (e.g. 6/14,
// whose VTT had only 3 cues even though Deepgram has thousands of words). Groups
// words into sentences on .!? boundaries (with a length cap so run-ons split).
// (Isaac, 6/29)
async function deepgramSentenceCues(pid) {
  try {
    const snap = await admin.firestore().collection('transcriptWords').doc(pid).get();
    const words = snap.exists ? snap.data()?.words : null;
    if (!Array.isArray(words) || !words.length) return [];
    const cues = [];
    let buf = [], startT = null;
    const flush = (endT) => {
      if (!buf.length) return;
      const text = buf.join(' ').replace(/\s+([.,;:!?])/g, '$1').trim();
      if (text) cues.push({ start: Math.round((startT ?? 0) * 1000) / 1000,
                            end: Math.round((endT ?? startT ?? 0) * 1000) / 1000, text });
      buf = []; startT = null;
    };
    for (const w of words) {
      const tok = w.w; if (!tok) continue;
      if (startT == null) startT = w.s ?? 0;
      buf.push(tok);
      const endsSentence = /[.!?]$/.test(tok);
      if (endsSentence || buf.length >= 30) flush(w.e ?? w.s ?? startT);
    }
    flush(words[words.length - 1]?.e);
    return cues;
  } catch { return []; }
}

async function cuesForAsset(asset) {
  const pid = asset.playback_ids?.[0]?.id;
  if (!pid) return [];
  const hit = _sermonCuesCache.get(pid);
  if (hit && Date.now() - hit.at < SERMON_CUES_TTL) return hit.cues;
  // Prefer Deepgram-word sentence cues (rich + accurate). Mux VTT is the
  // fallback, but for sermons it's often sparse/mislabeled, so if Deepgram has
  // MORE cues we use it. This is what makes the cross-sermon search find words
  // (e.g. "children") that the thin VTT missed. (Isaac, 6/29)
  const dgCues = await deepgramSentenceCues(pid);
  let vttCues = [];
  const textTrack = (asset.tracks || []).find(t => t.type === 'text' && t.text_type === 'subtitles' && t.status === 'ready');
  if (textTrack) {
    try {
      const r = await fetch(`https://stream.mux.com/${pid}/text/${textTrack.id}.vtt`);
      if (r.ok) {
        const { kept } = trimCues(parseVtt(await r.text()));
        vttCues = kept.map((c, i) => ({
          start: Math.round(c.start * 1000) / 1000,
          end: Math.round((kept[i + 1] ? kept[i + 1].start : c.start + 6) * 1000) / 1000,
          text: c.text,
        }));
      }
    } catch { /* ignore; fall back to Deepgram */ }
  }
  const cues = (dgCues.length > vttCues.length) ? dgCues : vttCues;
  _sermonCuesCache.set(pid, { cues, at: Date.now() });
  return cues;
}

// GET /api/sermons/search-transcripts?q=<word or phrase>&limit=<n>
// -> { ok:true, query, results:[ { playbackId, title, date, start, sentence } ] }
//    sorted newest-sermon-first, then by timestamp within a sermon.
app.get('/api/sermons/search-transcripts', async (req, res) => {
  try {
    const raw = (req.query.q || '').toString().trim();
    if (raw.length < 2) return res.json({ ok: false, message: 'Query too short.', results: [] });
    const needle = normForMatch(raw);
    if (!needle) return res.json({ ok: false, message: 'Query too short.', results: [] });
    const cap = Math.min(parseInt(req.query.limit, 10) || 200, 500);

    const all = await fetchAllMuxAssets();
    const sermons = (all || [])
      // Include EVERY sermon (not just ones with a ready Mux caption track) so
      // sermons whose VTT is sparse/preparing but have Deepgram words are still
      // searched. cuesForAsset prefers Deepgram cues + falls back to VTT, and
      // returns [] if neither exists. (Isaac, 6/29: 6/14 missed because its VTT
      // was nearly empty.)
      .filter(a => isSermonAsset(a))
      .sort((x, y) => Number(y.created_at) - Number(x.created_at));

    const results = [];
    for (const asset of sermons) {
      const pid = asset.playback_ids?.[0]?.id;
      if (!pid) continue;
      const pt = parsePassthrough(asset.passthrough);
      const title = pt.title || asset.meta?.title || 'Sermon';
      const date = asset.created_at ? new Date(asset.created_at * 1000).toISOString().slice(0, 10) : null;
      const cues = await cuesForAsset(asset);
      let lastNorm = null, lastStart = -999;
      for (let i = 0; i < cues.length; i++) {
        const c = cues[i];
        const norm = normForMatch(c.text);
        if (!norm.includes(needle)) { continue; }
        // Mux VTT emits many short, overlapping rolling-caption cues, so the same
        // sentence containing the word appears repeatedly a fraction of a second
        // apart. Collapse near-duplicate consecutive hits (same/contained text
        // within ~6s) so each is one clean occurrence. (Isaac, 6/29)
        const dup = lastNorm != null && (c.start - lastStart) < 6
          && (norm === lastNorm || norm.includes(lastNorm) || lastNorm.includes(norm));
        lastNorm = norm; lastStart = c.start;
        if (dup) continue;
        // FULL SENTENCE CONTEXT (Isaac, 6/29): VTT cues are short rolling
        // fragments, so expand around the match to the surrounding sentence by
        // walking neighboring cues out to sentence-ending punctuation (. ! ?).
        const exp = expandToSentence(cues, i, needle);
        // Start at the BEGINNING of the matched sentence (Isaac, 6/29), not the
        // cue where the word happens to land mid-sentence.
        results.push({ playbackId: pid, title, date, start: exp.start, sentence: exp.text });
        if (results.length >= cap) break;
      }
      if (results.length >= cap) break;
    }
    res.json({ ok: true, query: raw, count: results.length, results });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message, results: [] });
  }
});

// Per-WORD timings for accurate karaoke highlight + mid-track seeking (ADDITIVE).
// Reads the NEW transcriptWords/{playbackId} Firestore doc populated by Deepgram.
// This does NOT touch the Mux caption path. The app calls this OPTIONALLY: if the
// status isn't "ready", it falls back to the existing sentence-level /cues sync.
//
// GET /api/audio-transcript/:playbackId/words
//   -> { status: "ready", words: [{ w, s, e }] }
//      | { status: "none", message }   (no word data yet — app uses /cues)
app.get('/api/audio-transcript/:playbackId/words', async (req, res) => {
  try {
    const pid = req.params.playbackId;
    const snap = await admin.firestore().collection('transcriptWords').doc(pid).get();
    const words = snap.exists ? snap.data()?.words : null;
    if (!Array.isArray(words) || !words.length) {
      return res.json({ status: 'none', message: 'No word-level timing for this track.' });
    }
    res.json({ status: 'ready', words });
  } catch (e) {
    // On any error, report "none" so the app cleanly falls back to /cues.
    res.json({ status: 'none', message: e.message });
  }
});

// Locate the audio TIMESTAMP for a snippet of transcript text. Used by the
// app's "Listen" button on a highlighted note: given the highlight's quoted
// text, find where it was spoken so the player can seek there.
// GET /api/audio-transcript/:playbackId/locate?q=<snippet>
// -> { ok: true, start: <seconds> } | { ok: false, message }
function normForMatch(s) {
  return (s || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')   // drop punctuation
    .replace(/\s+/g, ' ')
    .trim();
}

// Build the FULL sentence around the matching cue. VTT rolling-caption cues are
// short fragments, so we stitch the matched cue with neighbors until we hit
// sentence-ending punctuation on each side (or a small cue budget). Returns a
// cleaned single-sentence (or two) string containing the searched term.
// (Isaac, 6/29)
function expandToSentence(cues, idx, needle) {
  const MAX_SPAN = 6; // cap how many cues out we reach each direction
  const endsSentence = (t) => /[.!?]\s*$/.test((t || '').trim());
  const startsSentence = (prevText) => endsSentence(prevText);
  // Walk LEFT: include earlier cues until the previous cue ends a sentence.
  let lo = idx;
  for (let k = 1; k <= MAX_SPAN; k++) {
    const prev = cues[idx - k];
    if (!prev) break;
    if (endsSentence(prev.text)) break; // previous cue completed a sentence
    lo = idx - k;
  }
  // Walk RIGHT: include later cues until THIS/last included cue ends a sentence.
  let hi = idx;
  if (!endsSentence(cues[idx].text)) {
    for (let k = 1; k <= MAX_SPAN; k++) {
      const cur = cues[idx + k];
      if (!cur) break;
      hi = idx + k;
      if (endsSentence(cur.text)) break;
    }
  }
  let joined = '';
  for (let i = lo; i <= hi; i++) {
    const piece = (cues[i]?.text || '').trim();
    if (!piece) continue;
    joined += (joined && !/\s$/.test(joined) ? ' ' : '') + piece;
  }
  joined = joined.replace(/\s+/g, ' ').trim();
  // Start time = first cue of the sentence so playback begins at the sentence start.
  const sentenceStart = cues[lo]?.start ?? cues[idx]?.start ?? 0;
  // Safety: if expansion somehow lost the term, fall back to the raw cue.
  if (!normForMatch(joined).includes(needle)) {
    return { text: (cues[idx]?.text || '').trim(), start: cues[idx]?.start ?? 0 };
  }
  return { text: joined, start: sentenceStart };
}

app.get('/api/audio-transcript/:playbackId/locate', async (req, res) => {
  try {
    const q = normForMatch(req.query.q || '');
    if (q.length < 4) return res.json({ ok: false, message: 'Query too short.' });
    const assetId = await assetIdForPlaybackId(req.params.playbackId);
    if (!assetId) return res.json({ ok: false, message: 'No asset for that playback ID.' });
    const current = await mux('GET', `/video/v1/assets/${assetId}`);
    const asset = current.data;
    const textTrack = (asset?.tracks || []).find(t => t.type === 'text' && t.text_type === 'subtitles' && t.status === 'ready');
    if (!textTrack) return res.json({ ok: false, message: 'No transcript captions ready.' });
    const pid = asset.playback_ids?.[0]?.id;
    const r = await fetch(`https://stream.mux.com/${pid}/text/${textTrack.id}.vtt`);
    if (!r.ok) return res.json({ ok: false, message: 'VTT fetch failed.' });
    const cues = parseVtt(await r.text());
    if (!cues.length) return res.json({ ok: false, message: 'Empty transcript.' });

    // Build one normalized string of all cue text, tracking which cue each
    // character belongs to (so a found offset maps back to a cue start time).
    let big = '';
    const owners = []; // owners[i] = index of cue that produced char i of big
    for (let ci = 0; ci < cues.length; ci++) {
      const piece = normForMatch(cues[ci].text);
      if (!piece) continue;
      const withSpace = (big.length ? ' ' : '') + piece;
      for (let k = 0; k < withSpace.length; k++) owners.push(ci);
      big += withSpace;
    }

    // Try the full snippet first, then progressively shorter leading slices so a
    // small transcript-vs-caption wording difference still finds the spot.
    const words = q.split(' ');
    let foundIdx = -1;
    for (const take of [words.length, 12, 8, 6, 4]) {
      if (take > words.length) continue;
      const probe = words.slice(0, take).join(' ');
      const idx = big.indexOf(probe);
      if (idx >= 0) { foundIdx = idx; break; }
    }
    if (foundIdx < 0) return res.json({ ok: false, message: 'Snippet not found in transcript.' });
    const cueIdx = owners[foundIdx] ?? 0;
    const start = cues[cueIdx]?.start ?? 0;
    return res.json({ ok: true, start });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Batch: kick off caption generation for every Korby-lecture audio track.
// Reads audio docs from Firestore, finds the Korby ones (artist/title/category),
// resolves each playback ID -> asset, and starts generate-subtitles where missing.
// Generic batch: kick off Mux caption generation for every audio asset whose
// title/artist matches a regex (?match=). Used to transcribe a whole series so
// its episodes become searchable + highlightable in the app.
// POST /api/audio-transcripts/generate?match=unfolding
app.post('/api/audio-transcripts/generate', async (req, res) => {
  try {
    const raw = (req.query.match || req.body?.match || '').toString().trim();
    if (!raw) return res.status(400).json({ error: 'Provide ?match=<regex> (matches title+artist).' });
    let re;
    try { re = new RegExp(raw, 'i'); } catch (e) { return res.status(400).json({ error: 'Bad regex: ' + e.message }); }
    const db = admin.firestore();
    const snap = await db.collection('audioAssets').get();
    const all = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    const matched = all.filter(a => re.test(`${a.title || ''} ${a.artist || ''}`));
    let started = 0, already = 0, failed = 0, noAsset = 0;
    const detail = [];
    for (const a of matched) {
      const m = /stream\.mux\.com\/([^.\/?]+)/.exec(a.audioUrl || '');
      if (!m) { failed++; detail.push({ title: a.title, result: 'no-playback-id' }); continue; }
      try {
        const assetId = await assetIdForPlaybackId(m[1]);
        if (!assetId) { noAsset++; detail.push({ title: a.title, result: 'no-asset' }); continue; }
        const cur = await mux('GET', `/video/v1/assets/${assetId}`);
        const asset = cur.data; const tracks = asset?.tracks || [];
        if (tracks.find(t => t.type === 'text' && t.text_type === 'subtitles')) { already++; detail.push({ title: a.title, result: 'already' }); continue; }
        const audioTrack = tracks.find(t => t.type === 'audio');
        if (!audioTrack) { failed++; detail.push({ title: a.title, result: 'no-audio-track' }); continue; }
        await mux('POST', `/video/v1/assets/${assetId}/tracks/${audioTrack.id}/generate-subtitles`, {
          generated_subtitles: [{ language_code: 'en', name: 'English (generated)' }],
        });
        started++; detail.push({ title: a.title, result: 'started' });
      } catch (e) { failed++; detail.push({ title: a.title, result: 'error: ' + e.message }); }
    }
    console.log(`[audio-transcripts/generate match=${raw}] started=${started} already=${already} noAsset=${noAsset} failed=${failed}`);
    res.json({ match: raw, total: matched.length, started, already, noAsset, failed, detail });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// COMPREHENSIVE BACKFILL (Isaac, 6/29): for EVERY non-music Mux asset, ensure
// transcript + word-timings exist by default. For each asset:
//   1) enable mp4_support:standard (so a static audio.m4a rendition exists for
//      Deepgram), 2) start Mux generate-subtitles if no text track yet,
//   3) generate + store Deepgram word-level timings (idempotent).
// Safe to re-run; skips music + assets without an audio track. Word-timing for
// freshly-enabled mp4 assets may need a second run once renditions finish
// (rendition prep is async) — the per-asset webhook also retries on
// static_renditions.ready.
// The sweep touches ~220 assets with Mux+Deepgram calls each, far longer than
// Railway's HTTP gateway timeout. So we run it in the BACKGROUND and return
// immediately; progress + final summary go to the server log and an in-memory
// status readable at GET /api/transcripts/backfill-all/status.
// POST /api/transcripts/backfill-all
let _backfillStatus = { running: false, startedAt: null, finishedAt: null, progress: null, summary: null };

async function _runBackfillAll() {
  _backfillStatus = { running: true, startedAt: new Date().toISOString(), finishedAt: null, progress: null, summary: null };
  try {
    let assets = [];
    let cursor = null;
    while (true) {
      const data = await mux('GET', cursor
        ? `/video/v1/assets?limit=100&order_direction=desc&cursor=${encodeURIComponent(cursor)}`
        : '/video/v1/assets?limit=100&order_direction=desc');
      if (data.data) assets.push(...data.data);
      if (data.next_cursor) cursor = data.next_cursor; else break;
    }
    let scanned = 0, mp4Enabled = 0, captionsStarted = 0, captionsHad = 0,
        wordsStored = 0, wordsHad = 0, wordsPending = 0, skippedMusic = 0,
        noAudio = 0, errors = 0;
    const total = assets.length;
    for (const asset of assets) {
      scanned++;
      _backfillStatus.progress = `${scanned}/${total}`;
      try {
        if (!shouldAutoTranscribe(asset)) { skippedMusic++; continue; }
        const tracks = asset.tracks || [];
        const audioTrack = tracks.find(t => t.type === 'audio');
        if (!audioTrack) { noAudio++; continue; }
        if (asset.mp4_support !== 'standard') {
          try { await mux('PUT', `/video/v1/assets/${asset.id}/mp4-support`, { mp4_support: 'standard' }); mp4Enabled++; }
          catch (e) { /* non-fatal */ }
        }
        if (tracks.some(t => t.type === 'text')) { captionsHad++; }
        else {
          try {
            await mux('POST', `/video/v1/assets/${asset.id}/tracks/${audioTrack.id}/generate-subtitles`, {
              generated_subtitles: [{ language_code: 'en', name: 'English (generated)' }],
            });
            captionsStarted++;
          } catch (e) { /* non-fatal */ }
        }
        const pid = asset.playback_ids?.[0]?.id;
        if (DEEPGRAM_KEY && pid) {
          try {
            const r = await generateAndStoreWords(pid, asset.id);
            if (r.ok && r.skipped) wordsHad++;
            else if (r.ok) wordsStored++;
            else wordsPending++;
          } catch (e) { wordsPending++; }
        }
      } catch (e) { errors++; }
    }
    const summary = { scanned, mp4Enabled, captionsStarted, captionsHad,
      wordsStored, wordsHad, wordsPending, skippedMusic, noAudio, errors };
    console.log('[backfill-all] DONE', JSON.stringify(summary));
    _backfillStatus = { running: false, startedAt: _backfillStatus.startedAt,
      finishedAt: new Date().toISOString(), progress: `${total}/${total}`, summary };
  } catch (e) {
    console.error('[backfill-all] FAILED', e.message);
    _backfillStatus = { ..._backfillStatus, running: false, finishedAt: new Date().toISOString(), summary: { error: e.message } };
  }
}

app.post('/api/transcripts/backfill-all', (req, res) => {
  if (_backfillStatus.running) {
    return res.json({ ok: true, alreadyRunning: true, progress: _backfillStatus.progress,
      note: 'Backfill already in progress. Poll GET /api/transcripts/backfill-all/status.' });
  }
  _runBackfillAll(); // fire-and-forget
  res.json({ ok: true, started: true,
    note: 'Backfill running in background. Poll GET /api/transcripts/backfill-all/status for progress + summary.' });
});

app.get('/api/transcripts/backfill-all/status', (req, res) => res.json(_backfillStatus));

// Backfill: swap EVERY sermon that has Deepgram words over to the Deepgram
// caption track (fixes existing sermons whose on-video CC says "[Music]").
// Async + status-pollable like the transcript backfill. (Isaac, 6/29)
let _ccSwapStatus = { running: false, startedAt: null, finishedAt: null, progress: null, summary: null };
async function _runDeepgramCcBackfill() {
  _ccSwapStatus = { running: true, startedAt: new Date().toISOString(), finishedAt: null, progress: null, summary: null };
  try {
    const all = await fetchAllMuxAssets();
    const sermons = (all || []).filter(a => isSermonAsset(a));
    let scanned = 0, swapped = 0, noWords = 0, errors = 0;
    const total = sermons.length;
    for (const a of sermons) {
      scanned++; _ccSwapStatus.progress = `${scanned}/${total}`;
      const pid = a.playback_ids?.[0]?.id;
      if (!pid) { errors++; continue; }
      try {
        await swapToDeepgramCaptions(pid, a.id);
        swapped++;
      } catch (e) {
        if (/No Deepgram words/.test(e.message)) noWords++; else errors++;
      }
    }
    const summary = { scanned, swapped, noWords, errors };
    console.log('[deepgram-cc-backfill] DONE', JSON.stringify(summary));
    _ccSwapStatus = { running: false, startedAt: _ccSwapStatus.startedAt, finishedAt: new Date().toISOString(), progress: `${total}/${total}`, summary };
  } catch (e) {
    console.error('[deepgram-cc-backfill] FAILED', e.message);
    _ccSwapStatus = { ..._ccSwapStatus, running: false, finishedAt: new Date().toISOString(), summary: { error: e.message } };
  }
}
app.post('/api/sermons/use-deepgram-captions/backfill-all', adminOnly, (req, res) => {
  if (_ccSwapStatus.running) return res.json({ ok: true, alreadyRunning: true, progress: _ccSwapStatus.progress });
  _runDeepgramCcBackfill();
  res.json({ ok: true, started: true, note: 'Swapping all sermons to Deepgram captions in background. Poll GET .../backfill-all/status.' });
});
app.get('/api/sermons/use-deepgram-captions/backfill-all/status', (req, res) => res.json(_ccSwapStatus));

app.post('/api/audio-transcripts/korby', async (req, res) => {
  try {
    const db = admin.firestore();
    const snap = await db.collection('audioAssets').get();
    const all = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    const korby = all.filter(a => /korby/i.test(`${a.title || ''} ${a.artist || ''}`));
    let started = 0, already = 0, failed = 0, noAsset = 0;
    const detail = [];
    for (const a of korby) {
      const m = /stream\.mux\.com\/([^.\/?]+)/.exec(a.audioUrl || '');
      if (!m) { failed++; detail.push({ title: a.title, result: 'no-playback-id' }); continue; }
      try {
        const assetId = await assetIdForPlaybackId(m[1]);
        if (!assetId) { noAsset++; detail.push({ title: a.title, result: 'no-asset' }); continue; }
        const cur = await mux('GET', `/video/v1/assets/${assetId}`);
        const asset = cur.data; const tracks = asset?.tracks || [];
        if (tracks.find(t => t.type === 'text' && t.text_type === 'subtitles')) { already++; detail.push({ title: a.title, result: 'already' }); continue; }
        const audioTrack = tracks.find(t => t.type === 'audio');
        if (!audioTrack) { failed++; detail.push({ title: a.title, result: 'no-audio-track' }); continue; }
        await mux('POST', `/video/v1/assets/${assetId}/tracks/${audioTrack.id}/generate-subtitles`, {
          generated_subtitles: [{ language_code: 'en', name: 'English (generated)' }],
        });
        started++; detail.push({ title: a.title, result: 'started' });
      } catch (e) { failed++; detail.push({ title: a.title, result: 'error: ' + e.message }); }
    }
    console.log(`[audio-transcripts/korby] started=${started} already=${already} noAsset=${noAsset} failed=${failed}`);
    res.json({ total: korby.length, started, already, noAsset, failed, detail });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Status map for the admin audio list: returns { statuses: { <audioAssetId>: 'ready'|'preparing'|'errored'|'none' } }
// so the UI can show a 📜 transcript icon next to assets that have a (ready/in-progress) transcript.
// Resolves each audio asset's Mux playback ID -> asset -> text track status, in small parallel batches.
app.get('/api/audio-transcript-status', async (req, res) => {
  try {
    const db = admin.firestore();
    const snap = await db.collection('audioAssets').get();
    const audio = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    const statuses = {};

    async function statusFor(a) {
      const m = /stream\.mux\.com\/([^.\/?]+)/.exec(a.audioUrl || '');
      if (!m) return 'none';
      try {
        const assetId = await assetIdForPlaybackId(m[1]);
        if (!assetId) return 'none';
        const cur = await mux('GET', `/video/v1/assets/${assetId}`);
        const tracks = cur?.data?.tracks || [];
        const textTrack = tracks.find(t => t.type === 'text' && t.text_type === 'subtitles');
        if (!textTrack) return 'none';
        if (textTrack.status === 'ready') return 'ready';
        if (textTrack.status === 'preparing') return 'preparing';
        if (textTrack.status === 'errored') return 'errored';
        return 'none';
      } catch { return 'none'; }
    }

    const CONCURRENCY = 6;
    for (let i = 0; i < audio.length; i += CONCURRENCY) {
      const batch = audio.slice(i, i + CONCURRENCY);
      const results = await Promise.all(batch.map(a => statusFor(a).then(s => [a.id, s])));
      for (const [id, s] of results) statuses[id] = s;
    }
    res.json({ statuses });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Backfill: start transcription for ready SERMON assets that don't have captions yet
app.post('/api/transcripts/backfill', async (req, res) => {
  try {
    let cursor = null, started = 0, skipped = 0, failed = 0;
    while (true) {
      const data = await mux('GET', cursor
        ? `/video/v1/assets?limit=100&cursor=${encodeURIComponent(cursor)}`
        : '/video/v1/assets?limit=100');
      for (const asset of data.data || []) {
        if (asset.status !== 'ready') { skipped++; continue; }
        if (!isSermonAsset(asset)) { skipped++; continue; }
        const tracks = asset.tracks || [];
        if (tracks.some(t => t.type === 'text')) { skipped++; continue; }
        const audioTrack = tracks.find(t => t.type === 'audio');
        if (!audioTrack) { skipped++; continue; }
        try {
          await mux('POST', `/video/v1/assets/${asset.id}/tracks/${audioTrack.id}/generate-subtitles`, {
            generated_subtitles: [{ language_code: 'en', name: 'English (generated)' }],
          });
          started++;
        } catch (e) {
          console.error(`[backfill] ${asset.id}: ${e.message}`);
          failed++;
        }
      }
      if (data.next_cursor) cursor = data.next_cursor; else break;
    }
    res.json({ started, skipped, failed });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// MANUAL backfill: generate Deepgram word-level timings for ready SERMON assets
// that don't already have a transcriptWords doc. ADDITIVE — only writes the new
// transcriptWords/{playbackId} collection; never touches Mux captions.
//
//   POST /api/transcript-words/backfill           — run it (admin only)
//   POST /api/transcript-words/backfill?dryRun=1  — just COUNT eligible assets
//   POST /api/transcript-words/backfill?force=1   — re-process even if data exists
//   POST /api/transcript-words/backfill?limit=5   — cap how many to process
// NOT auto-run anywhere. Trigger manually.
app.post('/api/transcript-words/backfill', adminOnly, async (req, res) => {
  if (!DEEPGRAM_KEY) return res.status(503).json({ error: 'DEEPGRAM_API_KEY not set' });
  const dryRun = req.query.dryRun === '1' || req.query.dryRun === 'true';
  const force = req.query.force === '1' || req.query.force === 'true';
  const limit = parseInt(req.query.limit, 10) || Infinity;
  try {
    const db = admin.firestore();
    let cursor = null, eligible = [], processed = 0, stored = 0, skipped = 0, failed = 0;
    // First pass: collect eligible sermon playback ids.
    while (true) {
      const data = await mux('GET', cursor
        ? `/video/v1/assets?limit=100&cursor=${encodeURIComponent(cursor)}`
        : '/video/v1/assets?limit=100');
      for (const asset of data.data || []) {
        if (asset.status !== 'ready') continue;
        if (!isSermonAsset(asset)) continue;
        const pid = asset.playback_ids?.[0]?.id;
        if (!pid) continue;
        eligible.push({ assetId: asset.id, pid });
      }
      if (data.next_cursor) cursor = data.next_cursor; else break;
    }
    if (dryRun) {
      return res.json({ dryRun: true, eligibleSermonAssets: eligible.length });
    }
    // Second pass: generate + store (sequential to be gentle on Deepgram).
    for (const { assetId, pid } of eligible) {
      if (processed >= limit) break;
      processed++;
      try {
        const r = await generateAndStoreWords(pid, assetId, { force });
        if (r.ok && r.skipped) skipped++;
        else if (r.ok) stored++;
        else { failed++; console.warn(`[words-backfill] ${assetId}: ${r.error}`); }
      } catch (e) {
        failed++;
        console.error(`[words-backfill] ${assetId}: ${e.message}`);
      }
    }
    res.json({ eligibleSermonAssets: eligible.length, processed, stored, skipped, failed, force });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Generate Deepgram word-level timings for EXPLICIT playback ids, regardless of
// the asset's sermon flag. Used to give a series the audio/text-tracking +
// karaoke highlight feature on demand (e.g. "Kenneth Korby Sermons"), the same
// way sermon assets get it automatically on upload. (Greg, 2026-06-19)
//   POST /api/transcript-words/generate   body: { playbackIds: ["...", ...], force?: true }
app.post('/api/transcript-words/generate', adminOnly, async (req, res) => {
  if (!DEEPGRAM_KEY) return res.status(503).json({ error: 'DEEPGRAM_API_KEY not set' });
  const force = req.query.force === '1' || req.query.force === 'true' || req.body?.force === true;
  const ids = Array.isArray(req.body?.playbackIds) ? req.body.playbackIds.filter(Boolean) : [];
  if (!ids.length) return res.status(400).json({ error: 'playbackIds array required' });
  const results = [];
  for (const pid of ids) {
    try {
      const assetId = await assetIdForPlaybackId(pid);
      const r = await generateAndStoreWords(pid, assetId, { force });
      results.push({ playbackId: pid, assetId: assetId || null, ...r });
      console.log(`[words-generate] ${pid}: ${r.ok ? (r.skipped ? 'skipped (existed)' : `stored ${r.count}`) : 'failed ' + r.error}`);
    } catch (e) {
      results.push({ playbackId: pid, ok: false, error: e.message });
      console.error(`[words-generate] ${pid}: ${e.message}`);
    }
  }
  res.json({ results });
});

// Transcript SOURCE + custom-transcript management (admin portal).
// transcriptOverrides/{pid} gains:
//   source: 'custom' → serve customText verbatim (uploaded or hand-edited)
//   customText: the user's transcript
//   generatedFallback unaffected: clearing custom reverts to deepgram/auto.

// Current transcript source/state for an asset so the portal can show the right
// toggle + prefill the editor. GET /api/assets/:id/transcript-source
app.get('/api/assets/:id/transcript-source', async (req, res) => {
  try {
    const current = await mux('GET', `/video/v1/assets/${req.params.id}`);
    const pid = current.data?.playback_ids?.[0]?.id;
    if (!pid) return res.status(400).json({ error: 'No playback ID' });
    const ov = await getTranscriptOverride(pid);
    const isCustom = !!(ov && ov.source === 'custom' && (ov.customText || '').trim());
    res.json({
      playbackId: pid,
      source: isCustom ? 'custom' : 'generated',
      hasCustom: !!(ov && (ov.customText || '').trim()),
      customText: ov?.customText || '',
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Save a custom/edited transcript and switch the asset to use it.
// PUT /api/assets/:id/custom-transcript  body: { text }
app.put('/api/assets/:id/custom-transcript', adminOnly, async (req, res) => {
  const text = (req.body?.text || '').toString();
  if (!text.trim()) return res.status(400).json({ error: 'text required (empty would clear — use DELETE instead)' });
  try {
    const current = await mux('GET', `/video/v1/assets/${req.params.id}`);
    const pid = current.data?.playback_ids?.[0]?.id;
    if (!pid) return res.status(400).json({ error: 'No playback ID' });
    await admin.firestore().collection('transcriptOverrides').doc(pid).set({
      source: 'custom', customText: text, assetId: req.params.id,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    delete _startOverrideCache[pid];
    console.log(`[custom-transcript] ${pid}: saved ${text.length} chars`);
    res.json({ ok: true, playbackId: pid, source: 'custom', chars: text.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Clear the custom transcript and revert to the generated source.
// DELETE /api/assets/:id/custom-transcript
app.delete('/api/assets/:id/custom-transcript', adminOnly, async (req, res) => {
  try {
    const current = await mux('GET', `/video/v1/assets/${req.params.id}`);
    const pid = current.data?.playback_ids?.[0]?.id;
    if (!pid) return res.status(400).json({ error: 'No playback ID' });
    const ref = admin.firestore().collection('transcriptOverrides').doc(pid);
    const snap = await ref.get();
    if (snap.exists) {
      const data = snap.data() || {};
      // Revert source: if a deepgram start override existed keep it, else clear source.
      const revertSource = (data.startSeconds != null && data.startSeconds !== '') ? 'deepgram' : admin.firestore.FieldValue.delete();
      await ref.set({
        source: revertSource,
        customText: admin.firestore.FieldValue.delete(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      }, { merge: true });
    }
    delete _startOverrideCache[pid];
    console.log(`[custom-transcript] ${pid}: cleared`);
    res.json({ ok: true, playbackId: pid, source: 'generated' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// PID-keyed variants of the custom-transcript endpoints, for AUDIO tracks in
// the app/portal that only carry a Mux playback id (no asset id on hand).
app.get('/api/transcript-source/:pid', async (req, res) => {
  try {
    const ov = await getTranscriptOverride(req.params.pid);
    const isCustom = !!(ov && ov.source === 'custom' && (ov.customText || '').trim());
    res.json({ playbackId: req.params.pid, source: isCustom ? 'custom' : 'generated',
      hasCustom: !!(ov && (ov.customText || '').trim()), customText: ov?.customText || '' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.put('/api/custom-transcript/:pid', adminOnly, async (req, res) => {
  const text = (req.body?.text || '').toString();
  if (!text.trim()) return res.status(400).json({ error: 'text required' });
  try {
    await admin.firestore().collection('transcriptOverrides').doc(req.params.pid).set({
      source: 'custom', customText: text,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    delete _startOverrideCache[req.params.pid];
    res.json({ ok: true, playbackId: req.params.pid, source: 'custom', chars: text.length });
  } catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete('/api/custom-transcript/:pid', adminOnly, async (req, res) => {
  try {
    const ref = admin.firestore().collection('transcriptOverrides').doc(req.params.pid);
    const snap = await ref.get();
    if (snap.exists) {
      const data = snap.data() || {};
      const revertSource = (data.startSeconds != null && data.startSeconds !== '') ? 'deepgram' : admin.firestore.FieldValue.delete();
      await ref.set({ source: revertSource, customText: admin.firestore.FieldValue.delete(),
        updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    }
    delete _startOverrideCache[req.params.pid];
    res.json({ ok: true, playbackId: req.params.pid, source: 'generated' });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Force a full transcript REDO from Deepgram for one playback id, with a manual
// start time. Use when Mux auto-captions are garbage (e.g. labeled the whole
// service "[ Singing ]"). Regenerates Deepgram words, stores a Deepgram override
// with the given start, and the transcript endpoints then serve the rebuilt text.
//   POST /api/transcript-redo  body: { playbackId, startSeconds? }
app.post('/api/transcript-redo', adminOnly, async (req, res) => {
  if (!DEEPGRAM_KEY) return res.status(503).json({ error: 'DEEPGRAM_API_KEY not set' });
  const pid = req.body?.playbackId;
  if (!pid) return res.status(400).json({ error: 'playbackId required' });
  const startSeconds = Math.max(0, Number(req.body?.startSeconds) || 0);
  try {
    const assetId = await assetIdForPlaybackId(pid);
    const r = await generateAndStoreWords(pid, assetId, { force: true });
    if (!r.ok) return res.status(502).json({ error: 'Deepgram failed: ' + r.error });
    await admin.firestore().collection('transcriptOverrides').doc(pid).set({
      startSeconds, source: 'deepgram', assetId: assetId || null,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    }, { merge: true });
    delete _startOverrideCache[pid];
    const built = await buildTranscriptFromWords(pid, startSeconds);
    console.log(`[transcript-redo] ${pid}: words=${r.count} startSeconds=${startSeconds} chars=${built?.text?.length || 0}`);
    res.json({ ok: true, playbackId: pid, assetId, words: r.count, startSeconds, chars: built?.text?.length || 0, preview: (built?.text || '').slice(0, 400) });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Regenerate the MUX caption (subtitle) track for one playback id: delete the
// existing text track(s) then start a fresh generate-subtitles pass. The sermon
// SEARCH reads cue timings from this Mux VTT, so use this when a week's search
// results show misaligned timestamps (e.g. 6/28, whose track came from the
// corrupted live-caption feed). Fresh Mux subtitles realign the cue times.
// Also clears the cached cues for that pid. (Isaac, 6/29)
//   POST /api/sermons/regenerate-captions  body: { playbackId }
app.post('/api/sermons/regenerate-captions', adminOnly, async (req, res) => {
  const pid = req.body?.playbackId;
  if (!pid) return res.status(400).json({ error: 'playbackId required' });
  try {
    const assetId = await assetIdForPlaybackId(pid);
    if (!assetId) return res.status(404).json({ error: 'No asset for that playback id' });
    const asset = (await mux('GET', `/video/v1/assets/${assetId}`)).data;
    const tracks = asset?.tracks || [];
    const textTracks = tracks.filter(t => t.type === 'text' && t.text_type === 'subtitles');
    let deleted = 0;
    for (const t of textTracks) {
      try { await mux('DELETE', `/video/v1/assets/${assetId}/tracks/${t.id}`); deleted++; }
      catch (e) { /* non-fatal */ }
    }
    const audioTrack = tracks.find(t => t.type === 'audio');
    if (!audioTrack) return res.status(422).json({ error: 'No audio track on asset' });
    // Ensure mp4 rendition for good measure (Deepgram path + downloads).
    if (asset.mp4_support !== 'standard') {
      try { await mux('PUT', `/video/v1/assets/${assetId}/mp4-support`, { mp4_support: 'standard' }); } catch (e) {}
    }
    await mux('POST', `/video/v1/assets/${assetId}/tracks/${audioTrack.id}/generate-subtitles`, {
      generated_subtitles: [{ language_code: 'en', name: 'English (generated)' }],
    });
    _sermonCuesCache.delete(pid);
    console.log(`[regenerate-captions] ${pid}: deleted ${deleted} old text track(s), started fresh subtitles`);
    res.json({ ok: true, playbackId: pid, assetId, deletedTracks: deleted,
      note: 'Fresh Mux subtitles generating (~minutes). Search timings realign once status=ready.' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Ensure the MOST RECENT sermon recording has a Deepgram transcript.
// Idempotent safety-net for the Sunday-afternoon cron (in case the webhook
// pipeline misses an event). Steps: find newest sermon asset → if it already
// has Deepgram words, done → else enable mp4_support and, once the audio
// rendition is ready, generate words (Deepgram). Returns a status the caller
// can use to decide whether to retry or announce.
//   POST /api/sermons/ensure-latest-transcript  body: { sinceHours?: 12 }
//   -> { status: 'ready'|'preparing'|'generating'|'none', ... }
app.post('/api/sermons/ensure-latest-transcript', adminOnly, async (req, res) => {
  if (!DEEPGRAM_KEY) return res.status(503).json({ error: 'DEEPGRAM_API_KEY not set' });
  const sinceHours = Number(req.body?.sinceHours) || 12;
  const cutoff = Date.now() / 1000 - sinceHours * 3600;
  try {
    // Find the newest sermon asset within the window.
    const data = await mux('GET', '/video/v1/assets?limit=20');
    const sermons = (data.data || [])
      .filter(a => isSermonAsset(a) && Number(a.created_at) >= cutoff)
      .sort((x, y) => Number(y.created_at) - Number(x.created_at));
    if (!sermons.length) return res.json({ status: 'none', message: `No sermon asset in the last ${sinceHours}h.` });
    const asset = sermons[0];
    const pid = asset.playback_ids?.[0]?.id;
    if (!pid) return res.json({ status: 'none', message: 'Sermon asset has no playback id.' });

    // Already transcribed?
    const existing = await admin.firestore().collection('transcriptWords').doc(pid).get();
    if (existing.exists && (existing.data()?.words || []).length) {
      const built = await buildTranscriptFromWords(pid, null);
      return res.json({ status: 'ready', playbackId: pid, assetId: asset.id,
        words: existing.data().words.length, chars: built?.text?.length || 0 });
    }

    // Make sure mp4_support is on so a rendition will generate.
    if (asset.mp4_support !== 'standard') {
      await mux('PUT', `/video/v1/assets/${asset.id}/mp4-support`, { mp4_support: 'standard' });
    }
    // Is an audio-capable rendition ready yet? (Deepgram reads audio.m4a/low.mp4.)
    let renditionReady = false;
    for (const f of ['audio.m4a', 'low.mp4', 'medium.mp4']) {
      try {
        const head = await fetch(`https://stream.mux.com/${pid}/${f}`, { method: 'HEAD' });
        if (head.ok) { renditionReady = true; break; }
      } catch { /* keep checking */ }
    }
    if (!renditionReady) {
      return res.json({ status: 'preparing', playbackId: pid, assetId: asset.id,
        message: 'mp4 rendition still preparing; retry shortly.' });
    }
    // Rendition ready — generate Deepgram words now (sermons default to Deepgram
    // transcript with auto-detected start, so no override needed).
    const r = await generateAndStoreWords(pid, asset.id, { force: true });
    if (!r.ok) return res.status(502).json({ status: 'error', playbackId: pid, error: r.error });
    const built = await buildTranscriptFromWords(pid, null);
    console.log(`[ensure-sermon-transcript] ${pid}: words=${r.count} chars=${built?.text?.length || 0}`);
    return res.json({ status: 'ready', playbackId: pid, assetId: asset.id,
      words: r.count, chars: built?.text?.length || 0 });
  } catch (e) {
    res.status(500).json({ status: 'error', error: e.message });
  }
});

// Cleanup: delete generated caption tracks from NON-sermon assets
app.post('/api/transcripts/cleanup-non-sermons', async (req, res) => {
  try {
    let cursor = null, removed = 0, kept = 0, failed = 0;
    while (true) {
      const data = await mux('GET', cursor
        ? `/video/v1/assets?limit=100&cursor=${encodeURIComponent(cursor)}`
        : '/video/v1/assets?limit=100');
      for (const asset of data.data || []) {
        const textTracks = (asset.tracks || []).filter(t => t.type === 'text');
        if (!textTracks.length) continue;
        if (isSermonAsset(asset)) { kept += textTracks.length; continue; }
        for (const tt of textTracks) {
          try {
            await mux('DELETE', `/video/v1/assets/${asset.id}/tracks/${tt.id}`);
            removed++;
          } catch (e) {
            console.error(`[cleanup] ${asset.id}/${tt.id}: ${e.message}`);
            failed++;
          }
        }
      }
      if (data.next_cursor) cursor = data.next_cursor; else break;
    }
    res.json({ removed, kept, failed });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Fetch KJV verse text for a reference like "2 Timothy 4:1-3" (cached)
const _kjvCache = {}; // reference -> text
async function fetchKjvText(ref) {
  if (_kjvCache[ref] !== undefined) return _kjvCache[ref];
  try {
    const q = encodeURIComponent(ref.replace(/\s+/g, ' '));
    const r = await fetch(`https://bible-api.com/${q}?translation=kjv`);
    if (!r.ok) { _kjvCache[ref] = null; return null; }
    const d = await r.json();
    const text = (d.text || '').replace(/\s*\n\s*/g, ' ').trim();
    _kjvCache[ref] = text || null;
    return _kjvCache[ref];
  } catch {
    _kjvCache[ref] = null;
    return null;
  }
}

// Detect where continuous preaching begins in a VTT cue list.
// Singing/music sections have sparse, short, repetitive cues; preaching is dense
// (>=80 words/min), long sentences (>=6 words/cue), and non-repetitive (>=70% unique cues).
// Returns the start time (s) of the first run of 3 consecutive "speechy" minutes, or 0 if not found.
function detectSermonStart(cues) {
  const buckets = {};
  for (const c of cues) {
    const text = c.text.replace(/[♪♫]/g, '').replace(/[\[\(][^\]\)]*[\]\)]/g, '').trim();
    const b = Math.floor(c.start / 60);
    (buckets[b] = buckets[b] || []).push(text);
  }
  const speechy = (b) => {
    const texts = (buckets[b] || []).filter(Boolean);
    if (!texts.length) return false;
    const words = texts.reduce((n, t) => n + t.split(/\s+/).length, 0);
    const avg = words / texts.length;
    const uniq = new Set(texts.map(t => t.toLowerCase())).size / texts.length;
    return words >= 80 && avg >= 6 && uniq >= 0.7;
  };
  // Moderate speech — used to walk the start back so we don't clip the sermon opening
  // (scripture reading / soft-spoken intros can dip below the strict threshold)
  const semiSpeechy = (b) => {
    const texts = (buckets[b] || []).filter(Boolean);
    if (!texts.length) return false;
    const words = texts.reduce((n, t) => n + t.split(/\s+/).length, 0);
    const avg = words / texts.length;
    const uniq = new Set(texts.map(t => t.toLowerCase())).size / texts.length;
    return words >= 30 && avg >= 5 && uniq >= 0.7;
  };
  const maxB = Math.max(...Object.keys(buckets).map(Number), 0);
  for (let b = 0; b <= maxB - 2; b++) {
    if (speechy(b) && speechy(b + 1) && speechy(b + 2)) {
      // Lock-on found; walk back over any contiguous semi-speechy lead-in (up to 10 min)
      let s = b;
      let gap = 0;
      for (let k = b - 1; k >= 0 && b - k <= 10; k--) {
        if (semiSpeechy(k)) { s = k; gap = 0; }
        else if (++gap > 1) break; // allow a single quiet minute inside the lead-in
      }
      return s * 60;
    }
  }
  return 0; // detection failed — keep everything
}

// Trim + clean a parsed VTT cue list down to the SAME kept cues that
// buildCleanTranscript() uses for its text. Returns { kept, sermonStart } where
// `kept` is an array of { start, text } in order: pre-sermon music/singing
// dropped, non-speech tags dropped, consecutive duplicates collapsed, and the
// same end-of-sentence period inference applied to each cue's text.
//
// This is the shared core so the live-sync /cues endpoint stays word-for-word
// aligned with the cleaned transcript blob the app renders (reflowSentences()
// only rejoins/re-splits the SAME words, preserving order).
function trimCues(cues) {
  const sermonStart = detectSermonStart(cues);
  // De-duplicate consecutive identical cues (Mux VTT often repeats lines),
  // then infer missing sentence ends at cue boundaries: when a cue ends without
  // punctuation, the next cue starts with a capital, and the last word isn't a
  // connector, the speaker almost certainly ended a sentence the AI didn't punctuate.
  const CONNECTORS = new Set(['and','but','or','to','of','the','a','an','in','on','for','with','that','which','who','as','is','are','was','were','be','by','at','from','so','because','if','when','my','our','your','his','her','their','this','these','those','very','more','most','can','could','will','would','shall','should','has','have','had','not','no','it','he','she','we','they','you','i','what','how','than','then']);
  // Non-speech marker cue: [MUSIC PLAYING], ( Singing ), [Applause], etc. (any bracketed/parened tag,
  // optionally with trailing punctuation like "." or ","). Drop these BEFORE de-dupe and BEFORE the
  // punctuation-inference pass, otherwise a tag that gets a period appended (e.g. "[ Singing ].")
  // will no longer match the end-of-line stripper in cleanTranscript() and will leak into the output.
  const NONSPEECH_RE = /^[\[\(]\s*(music|applause|laughter|blank[_ ]?audio|singing|instrumental|inaudible|silence|noise|background\s+noise|crowd|coughing|sighs?)[^\]\)]*[\]\)][.,;:!?]*$/i;
  const kept = [];
  let prev = null;
  for (const c of cues) {
    if (c.start < sermonStart) continue;
    const t = c.text.trim();
    if (!t || t === prev) continue;
    if (NONSPEECH_RE.test(t)) continue;
    kept.push({ start: c.start, text: t });
    prev = t;
  }
  // Apply the same end-of-sentence period inference, in place, so the kept cue
  // text matches the words that flow into the cleaned blob.
  for (let i = 0; i < kept.length; i++) {
    const t = kept[i].text;
    const nxt = kept[i + 1]?.text;
    if (nxt && t && !/[.!?,;:]$/.test(t) && /^[A-Z]/.test(nxt)) {
      const lastWord = (t.split(/\s+/).pop() || '').toLowerCase().replace(/["']/g, '');
      if (lastWord && !CONNECTORS.has(lastWord)) kept[i].text = t + '.';
    }
  }
  return { kept, sermonStart };
}

// Build a cleaned transcript (music stripped, singing portion removed) from an asset's VTT.
// Returns { text, sermonStart } or null if no transcript ready.
async function buildCleanTranscript(asset) {
  const pid = asset.playback_ids?.[0]?.id;
  const textTrack = (asset.tracks || []).find(t => t.type === 'text' && t.text_type === 'subtitles' && t.status === 'ready');
  if (!pid || !textTrack) return null;
  const r = await fetch(`https://stream.mux.com/${pid}/text/${textTrack.id}.vtt`);
  if (!r.ok) throw new Error('VTT fetch failed: ' + r.status);
  const cues = parseVtt(await r.text());
  const { kept, sermonStart } = trimCues(cues);
  return { text: reflowSentences(cleanTranscript(kept.map(c => c.text).join('\n'))), sermonStart };
}

// ── Per-asset transcript start override + Deepgram-built transcript ───────────
// Some livestreams confuse Mux auto-captioning (e.g. it labels the whole 2-hour
// service as "[ Singing ]" and only captures the closing prayer). For those we
// rebuild the transcript from Deepgram WORD timings (which actually transcribe
// the spoken sermon) and optionally force a manual start time so the pre-sermon
// worship is dropped. Overrides live in Firestore: transcriptOverrides/{pid}
//   { startSeconds: <number>, source: 'deepgram'|'auto' }
const _startOverrideCache = {}; // pid -> { startSeconds, source } | null  (5 min TTL)
async function getTranscriptOverride(playbackId) {
  const c = _startOverrideCache[playbackId];
  if (c && Date.now() - c._t < 5 * 60 * 1000) return c.val;
  try {
    const snap = await admin.firestore().collection('transcriptOverrides').doc(playbackId).get();
    const val = snap.exists ? (snap.data() || null) : null;
    _startOverrideCache[playbackId] = { val, _t: Date.now() };
    return val;
  } catch {
    return null;
  }
}

// Detect where continuous preaching begins from Deepgram WORD timings. Same
// idea as detectSermonStart (which works on VTT cues): worship/music produces
// sparse, short, repetitive word runs per minute; preaching is dense and varied.
// Returns the start time (s) of the first run of 3 dense "speechy" minutes,
// walking back over a soft lead-in, or 0 if detection fails (keep everything).
function detectSermonStartFromWords(words) {
  if (!Array.isArray(words) || !words.length) return 0;
  const buckets = {};
  for (const w of words) {
    if (!w.w) continue;
    const b = Math.floor((w.s ?? 0) / 60);
    (buckets[b] = buckets[b] || []).push(w.w);
  }
  // Words-per-minute thresholds: real preaching is ~110-160 wpm; worship/quiet
  // intros are far sparser. Require a sustained run so a stray dense minute in
  // the music set doesn't trigger early.
  const wpm = (b) => (buckets[b] || []).length;
  const dense = (b) => wpm(b) >= 90;
  const semiDense = (b) => wpm(b) >= 45;
  const maxB = Math.max(...Object.keys(buckets).map(Number), 0);
  for (let b = 0; b <= maxB - 2; b++) {
    if (dense(b) && dense(b + 1) && dense(b + 2)) {
      let s = b, gap = 0;
      for (let k = b - 1; k >= 0 && b - k <= 10; k--) {
        if (semiDense(k)) { s = k; gap = 0; }
        else if (++gap > 1) break;
      }
      return s * 60;
    }
  }
  return 0;
}

// Build a cleaned, reflowed transcript from stored Deepgram words, dropping any
// word that starts before `startSeconds`. Returns { text, sermonStart } or null.
// When `startSeconds` is null/undefined, auto-detect the sermon start from the
// words (trims pre-sermon worship automatically).
async function buildTranscriptFromWords(playbackId, startSeconds = null) {
  const snap = await admin.firestore().collection('transcriptWords').doc(playbackId).get();
  const words = snap.exists ? snap.data()?.words : null;
  if (!Array.isArray(words) || !words.length) return null;
  const start = (startSeconds == null) ? detectSermonStartFromWords(words) : startSeconds;
  const kept = words.filter(w => (w.s ?? 0) >= start && w.w);
  if (!kept.length) return null;
  const blob = kept.map(w => w.w).join(' ').replace(/\s+([.,;:!?])/g, '$1');
  return { text: reflowSentences(blob), sermonStart: start };
}

// Resolve the Deepgram start for an asset: explicit override wins; otherwise
// auto-detect from words (null signals auto to buildTranscriptFromWords).
async function resolveDeepgramStart(pid, ov) {
  if (ov && ov.startSeconds != null && ov.startSeconds !== '') return Number(ov.startSeconds) || 0;
  return null; // auto-detect
}

// Preferred transcript builder. Deepgram is the DEFAULT source for sermons
// (Sunday livestream recordings) because Mux auto-captions routinely mislabel
// the worship set as "[ Singing ]" and miss the preaching. For sermons we build
// from Deepgram words with an auto-detected start (or an explicit override).
// A manual deepgram override forces the Deepgram path for ANY asset. Everything
// else falls back to the Mux VTT path. Returns { text, sermonStart } or null.
async function buildBestTranscript(asset) {
  const pid = asset.playback_ids?.[0]?.id;
  if (pid) {
    const ov = await getTranscriptOverride(pid);
    // A custom/edited transcript (uploaded or hand-edited in the admin portal)
    // wins over everything. Stored as source:'custom' + customText. sermonStart
    // 0 so no "music removed" note is prepended to user-authored text.
    if (ov && ov.source === 'custom' && (ov.customText || '').trim()) {
      return { text: ov.customText, sermonStart: 0 };
    }
    const useDeepgram = (ov && ov.source === 'deepgram') || isSermonAsset(asset);
    if (useDeepgram) {
      const start = await resolveDeepgramStart(pid, ov);
      const fromWords = await buildTranscriptFromWords(pid, start);
      if (fromWords) return fromWords;
    }
  }
  return buildCleanTranscript(asset);
}

// Reflow caption-cue lines into complete sentences and readable paragraphs.
// Cue boundaries split sentences mid-stream; join everything, then break on
// sentence endings, grouping ~4 sentences per paragraph.
function reflowSentences(text) {
  const joined = text
    .split('\n')
    .map(l => l.trim())
    .filter(Boolean)
    .join(' ')
    .replace(/\s+/g, ' ');
  // Split into sentences on . ! ? — but transcripts sometimes lack punctuation for long
  // stretches, so any "sentence" longer than ~60 words gets force-chunked by word count.
  const rawSentences = joined.match(/[^.!?]*[.!?]+(?:["')\]]+)?(?:\s|$)/g) || [];
  const matchedLen = rawSentences.join('').length;
  if (joined.length - matchedLen > 5) rawSentences.push(joined.slice(matchedLen)); // unpunctuated tail
  const sentences = [];
  for (const s of rawSentences) {
    const words = s.trim().split(/\s+/).filter(Boolean);
    if (words.length <= 60) {
      if (words.length) sentences.push(words.join(' '));
    } else {
      for (let i = 0; i < words.length; i += 50) sentences.push(words.slice(i, i + 50).join(' '));
    }
  }
  const paras = [];
  let cur = [];
  let curWords = 0;
  for (const t of sentences) {
    cur.push(t);
    curWords += t.split(/\s+/).length;
    if (cur.length >= 4 || curWords >= 120) {
      paras.push(cur.join(' '));
      cur = [];
      curWords = 0;
    }
  }
  if (cur.length) paras.push(cur.join(' '));
  return paras.join('\n\n');
}

// Strip music/non-speech noise from a transcript: ♪ lines, [MUSIC PLAYING], (Applause), [BLANK_AUDIO], etc.
function cleanTranscript(text) {
  return text
    .split('\n')
    .filter(line => {
      const t = line.trim();
      if (!t) return true; // keep blank lines for now, collapsed below
      if (/^[\s♪♫\*\-~]+$/.test(t)) return false;                      // music-note-only lines
      if (/^[\[\(]\s*(music|applause|laughter|blank[_ ]?audio|singing|instrumental|inaudible|silence)[^\]\)]*[\]\)]$/i.test(t)) return false; // [MUSIC PLAYING], (Applause)...
      return true;
    })
    .join('\n')
    .replace(/[♪♫]/g, '')           // stray notes inside lines
    .replace(/\n{3,}/g, '\n\n')     // collapse runs of blank lines
    .replace(/^\s+/, '');           // trim leading whitespace/newlines
}

function fmtTimestamp(secs) {
  const h = Math.floor(secs / 3600), m = Math.floor((secs % 3600) / 60), s = Math.floor(secs % 60);
  return (h ? h + ':' + String(m).padStart(2, '0') : String(m)) + ':' + String(s).padStart(2, '0');
}

// Build the "Scripture References" header block for a transcript
async function buildRefsHeader(asset) {
  try {
    const refs = await extractScriptureRefs(asset);
    if (!refs || !refs.length) return '';
    // De-dupe by reference string for the header list (keep first timestamp)
    const seen = new Set();
    const unique = refs.filter(r => !seen.has(r.reference) && seen.add(r.reference));
    const lines = ['SCRIPTURE REFERENCES', '====================', ''];
    for (const ref of unique) {
      const verse = await fetchKjvText(ref.reference);
      lines.push(`[${fmtTimestamp(ref.timestamp)}] ${ref.reference}`);
      if (verse) lines.push(`  "${verse}" (KJV)`);
      lines.push('');
    }
    lines.push('====================', 'TRANSCRIPT', '====================', '');
    return lines.join('\n');
  } catch (e) {
    console.error('[transcript] refs header failed:', e.message);
    return '';
  }
}

// Download transcript as a .txt attachment (with scripture reference header)
app.get('/api/assets/:id/transcript.txt', async (req, res) => {
  try {
    const current = await mux('GET', `/video/v1/assets/${req.params.id}`);
    const asset = current.data;
    const pid = asset?.playback_ids?.[0]?.id;
    const textTrack = (asset?.tracks || []).find(t => t.type === 'text' && t.text_type === 'subtitles' && t.status === 'ready');
    if (!pid || !textTrack) return res.status(404).send('Transcript not ready');
    const clean = await buildBestTranscript(asset);
    if (!clean) return res.status(404).send('Transcript not ready');
    const header = await buildRefsHeader(asset);
    const note = clean.sermonStart > 0 ? `(Pre-sermon music/singing removed — sermon begins ~${fmtTimestamp(clean.sermonStart)} in the recording)\n\n` : '';
    const title = (asset.meta?.title || 'transcript').replace(/[^a-z0-9 \-_]/gi, '').trim() || 'transcript';
    res.set('Content-Type', 'text/plain; charset=utf-8');
    res.set('Content-Disposition', `attachment; filename="${title}.txt"`);
    res.send(header + note + clean.text);
  } catch (e) {
    res.status(500).send(e.message);
  }
});

// ── Scripture Reference Extraction ───────────────────────────────────────────
const BIBLE_BOOKS = '(?:Genesis|Exodus|Leviticus|Numbers|Deuteronomy|Joshua|Judges|Ruth|(?:1st|2nd|First|Second|1|2|I|II) Samuel|(?:1st|2nd|First|Second|1|2|I|II) Kings|(?:1st|2nd|First|Second|1|2|I|II) Chronicles|Ezra|Nehemiah|Esther|Job|Psalms?|Proverbs|Ecclesiastes|Song of Solomon|Isaiah|Jeremiah|Lamentations|Ezekiel|Daniel|Hosea|Joel|Amos|Obadiah|Jonah|Micah|Nahum|Habakkuk|Zephaniah|Haggai|Zechariah|Malachi|Matthew|Mark|Luke|John|Acts|Romans|(?:1st|2nd|First|Second|1|2|I|II) Corinthians|Galatians|Ephesians|Philippians|Colossians|(?:1st|2nd|First|Second|1|2|I|II) Thessalonians|(?:1st|2nd|First|Second|1|2|I|II) Timothy|Titus|Philemon|Hebrews|James|(?:1st|2nd|First|Second|1|2|I|II) Peter|(?:1st|2nd|3rd|First|Second|Third|1|2|3|I|II|III) John|Jude|Revelation)';
const SCRIPTURE_RE = new RegExp('\\b(' + BIBLE_BOOKS + ')\\s+(?:chapter\\s+)?(\\d{1,3})(?:\\s*[:,]\\s*|\\s+(?:and\\s+)?verses?\\s+)(\\d{1,3})(?:\\s*[-\u2013]\\s*(\\d{1,3}))?', 'gi');

// ── Spoken-number scripture parsing ──────────────────────────────────────────
// Catches refs spoken aloud as words, e.g. "Romans six three", "John chapter
// three verse sixteen", "first Corinthians thirteen four through seven". Deepgram
// sometimes leaves chapter/verse as word-numbers; this second pass converts them.
const NUM_WORDS = {
  zero:0, one:1, two:2, three:3, four:4, five:5, six:6, seven:7, eight:8, nine:9,
  ten:10, eleven:11, twelve:12, thirteen:13, fourteen:14, fifteen:15, sixteen:16,
  seventeen:17, eighteen:18, nineteen:19, twenty:20, thirty:30, forty:40, fifty:50,
  sixty:60, seventy:70, eighty:80, ninety:90,
};
// Parse a run of number-words into an integer (handles "twenty three", "twenty-three",
// "one hundred", "one hundred nineteen"). Returns { value, wordCount } or null.
function parseSpokenNumber(tokens, i) {
  let total = 0, cur = 0, used = 0, sawHundred = false;
  while (i + used < tokens.length) {
    const w = tokens[i + used].toLowerCase().replace(/[^a-z]/g, '');
    if (w in NUM_WORDS) { cur += NUM_WORDS[w]; used++; continue; }
    if (w === 'hundred' && (cur > 0 || used > 0)) { cur = (cur || 1) * 100; sawHundred = true; used++; continue; }
    break;
  }
  if (!used) return null;
  total = cur;
  if (total < 1 || total > 176) return null; // valid chapter/verse range guard
  return { value: total, wordCount: used };
}
// Spoken book pattern reuses BIBLE_BOOKS but tolerates spoken ordinals already inside it.
const SPOKEN_BOOK_RE = new RegExp('\\b(' + BIBLE_BOOKS + ')\\b', 'gi');
// Longer words first so e.g. "sixteen" is matched before "six" (alternation is leftmost-first).
const SPOKEN_NUM_TOKEN = '(?:seventeen|seventy|sixteen|sixty|nineteen|ninety|fourteen|forty|thirteen|thirty|fifteen|fifty|eighteen|eighty|twelve|twenty|eleven|hundred|zero|seven|eight|three|four|five|nine|six|ten|two|one)';
// book [chapter] <num...> [verse|colon] <num...> [through|to|dash <num...>]
const SPOKEN_REF_RE = new RegExp(
  '\\b(' + BIBLE_BOOKS + ')\\s+(?:chapter\\s+)?' +
  '((?:' + SPOKEN_NUM_TOKEN + '[\\s-]*)+)' +
  '(?:\\s*(?:verse|verses|colon)\\s+|\\s+)' +
  '((?:' + SPOKEN_NUM_TOKEN + '[\\s-]*)+)' +
  '(?:(?:\\s*(?:through|thru|to|[-\u2013])\\s*)((?:' + SPOKEN_NUM_TOKEN + '[\\s-]*)+))?',
  'gi');
function wordsToInt(phrase) {
  const toks = (phrase || '').trim().split(/[\s-]+/).filter(Boolean);
  const r = parseSpokenNumber(toks, 0);
  return r ? r.value : null;
}
// Scan text for spoken-number refs; returns [{ reference, book, chapter, verse, verseEnd, index, spoken }].
function matchSpokenRefs(text) {
  const out = [];
  SPOKEN_REF_RE.lastIndex = 0;
  let m;
  while ((m = SPOKEN_REF_RE.exec(text)) !== null) {
    const book = normalizeBook(m[1]);
    const chapter = wordsToInt(m[2]);
    const verse = wordsToInt(m[3]);
    const verseEnd = m[4] ? wordsToInt(m[4]) : null;
    if (!chapter || !verse) continue;
    if (!isPlausibleRef(book, chapter, verse)) continue; // drop mis-heard junk (e.g. "Luke 69:2")
    const reference = `${book} ${chapter}:${verse}` + (verseEnd ? `-${verseEnd}` : '');
    out.push({ reference, book, chapter, verse, verseEnd, index: m.index, spoken: m[0] });
  }
  return out;
}

// Normalize spoken book names: "First Peter"/"1st Peter"/"I Peter" → "1 Peter"
function normalizeBook(book) {
  return book
    .replace(/^(?:First|1st|I)\s+/i, '1 ')
    .replace(/^(?:Second|2nd|II)\s+/i, '2 ')
    .replace(/^(?:Third|3rd|III)\s+/i, '3 ')
    .replace(/^Psalm$/i, 'Psalms')
    .replace(/\b\w/g, c => c.toUpperCase())
    .replace(/ Of /g, ' of ');
}

// Chapter counts per book (KJV) — used to reject impossible spoken-number refs
// like "Luke 69:2" or "Proverbs 38:6" that come from mis-heard number sequences.
const BOOK_CHAPTERS = {
  'Genesis':50,'Exodus':40,'Leviticus':27,'Numbers':36,'Deuteronomy':34,'Joshua':24,'Judges':21,'Ruth':4,
  '1 Samuel':31,'2 Samuel':24,'1 Kings':22,'2 Kings':25,'1 Chronicles':29,'2 Chronicles':36,'Ezra':10,'Nehemiah':13,
  'Esther':10,'Job':42,'Psalms':150,'Proverbs':31,'Ecclesiastes':12,'Song of Solomon':8,'Isaiah':66,'Jeremiah':52,
  'Lamentations':5,'Ezekiel':48,'Daniel':12,'Hosea':14,'Joel':3,'Amos':9,'Obadiah':1,'Jonah':4,'Micah':7,'Nahum':3,
  'Habakkuk':3,'Zephaniah':3,'Haggai':2,'Zechariah':14,'Malachi':4,'Matthew':28,'Mark':16,'Luke':24,'John':21,
  'Acts':28,'Romans':16,'1 Corinthians':16,'2 Corinthians':13,'Galatians':6,'Ephesians':6,'Philippians':4,
  'Colossians':4,'1 Thessalonians':5,'2 Thessalonians':3,'1 Timothy':6,'2 Timothy':4,'Titus':3,'Philemon':1,
  'Hebrews':13,'James':5,'1 Peter':5,'2 Peter':3,'1 John':5,'2 John':1,'3 John':1,'Jude':1,'Revelation':22,
};
// True if `book chapter:verse` is a plausible KJV reference (chapter within range).
function isPlausibleRef(book, chapter, verse) {
  const max = BOOK_CHAPTERS[book];
  if (!max) return true; // unknown book name variant — don't over-reject
  return chapter >= 1 && chapter <= max && verse >= 1 && verse <= 176;
}

// Parse a Mux VTT transcript into [{start, text}] cues
function parseVtt(vtt) {
  const cues = [];
  for (const block of vtt.split(/\n\n+/)) {
    const lines = block.trim().split('\n').filter(Boolean);
    for (let i = 0; i < lines.length; i++) {
      const m = lines[i].match(/(\d+):(\d+):([\d.]+)\s*-->/);
      if (m) {
        const start = (+m[1]) * 3600 + (+m[2]) * 60 + parseFloat(m[3]);
        cues.push({ start, text: lines.slice(i + 1).join(' ') });
        break;
      }
    }
  }
  return cues;
}

// Extract scripture references with timestamps from an asset's transcript
// Extract scripture refs from Deepgram WORD timings (used when a Deepgram
// transcript override is set — the Mux VTT is unusable for those assets).
// Builds a rolling text window with each word's start time so refs keep an
// accurate timestamp, honoring the same manual start as the transcript.
async function extractScriptureRefsFromWords(playbackId, startSeconds = 0) {
  const snap = await admin.firestore().collection('transcriptWords').doc(playbackId).get();
  const words = (snap.exists ? snap.data()?.words : null) || [];
  const kept = words.filter(w => (w.s ?? 0) >= startSeconds && w.w);
  if (!kept.length) return [];
  const text = kept.map(w => w.w).join(' ');
  // Map a character offset in `text` back to the word's start time.
  const offsets = []; let pos = 0;
  for (const w of kept) { offsets.push({ at: pos, t: w.s ?? 0 }); pos += w.w.length + 1; }
  const tsForOffset = (idx) => {
    let lo = 0, hi = offsets.length - 1, ans = 0;
    while (lo <= hi) { const mid = (lo + hi) >> 1; if (offsets[mid].at <= idx) { ans = offsets[mid].t; lo = mid + 1; } else hi = mid - 1; }
    return Math.round(ans);
  };
  const refs = []; const seen = new Set();
  const pushRef = (refStr, book, chapter, verse, verseEnd, index, spoken) => {
    const ts = tsForOffset(index);
    const key = refStr + '|' + Math.floor(ts / 60);
    if (seen.has(key)) return;
    seen.add(key);
    refs.push({ reference: refStr, book, chapter, verse, verseEnd, timestamp: ts, spoken });
  };
  // Pass 1: digit-form refs ("Romans 6:3").
  SCRIPTURE_RE.lastIndex = 0;
  let m;
  while ((m = SCRIPTURE_RE.exec(text)) !== null) {
    const book = normalizeBook(m[1]);
    const chapter = +m[2], verse = +m[3], verseEnd = m[4] ? +m[4] : null;
    pushRef(`${book} ${chapter}:${verse}` + (verseEnd ? `-${verseEnd}` : ''), book, chapter, verse, verseEnd, m.index, m[0]);
  }
  // Pass 2: spoken-number refs ("Romans six three", "John chapter three verse sixteen").
  for (const s of matchSpokenRefs(text)) {
    pushRef(s.reference, s.book, s.chapter, s.verse, s.verseEnd, s.index, s.spoken);
  }
  // Order by timestamp so the header reads in sermon order.
  refs.sort((a, b) => a.timestamp - b.timestamp);
  return refs;
}

async function extractScriptureRefs(asset) {
  const pid = asset.playback_ids?.[0]?.id;
  // Scripture-reference header is a SUNDAY SERMON-only feature. For any other
  // content (audiobooks, series episodes, podcasts, etc.) never compile a refs
  // header — even if Mux captions happen to mention verses. Only proceed when
  // this is a sermon asset or has an explicit Deepgram override.
  {
    const ov = pid ? await getTranscriptOverride(pid) : null;
    if (!isSermonAsset(asset) && !(ov && ov.source === 'deepgram')) return null;
  }
  // Sermons (and any explicit Deepgram override): scan the Deepgram words rather
  // than the Mux VTT, which mislabels worship as singing. Start = override or
  // auto-detected sermon start (matches the transcript body).
  if (pid) {
    const ov = await getTranscriptOverride(pid);
    const useDeepgram = (ov && ov.source === 'deepgram') || isSermonAsset(asset);
    if (useDeepgram) {
      let start = await resolveDeepgramStart(pid, ov);
      if (start == null) {
        const snap = await admin.firestore().collection('transcriptWords').doc(pid).get();
        const words = snap.exists ? snap.data()?.words : null;
        start = detectSermonStartFromWords(words || []);
      }
      const refs = await extractScriptureRefsFromWords(pid, start);
      if (refs && refs.length) return refs;
      // If Deepgram produced no refs (e.g. words not ready yet) fall through to VTT.
    }
  }
  const textTrack = (asset.tracks || []).find(t => t.type === 'text' && t.text_type === 'subtitles' && t.status === 'ready');
  if (!pid || !textTrack) return null; // transcript not ready
  const r = await fetch(`https://stream.mux.com/${pid}/text/${textTrack.id}.vtt`);
  if (!r.ok) throw new Error('VTT fetch failed: ' + r.status);
  const cues = parseVtt(await r.text());
  // Scan cues in overlapping pairs so refs split across cue boundaries are caught
  const refs = [];
  const seen = new Set();
  for (let i = 0; i < cues.length; i++) {
    const text = cues[i].text + ' ' + (cues[i + 1]?.text || '');
    SCRIPTURE_RE.lastIndex = 0;
    let m;
    while ((m = SCRIPTURE_RE.exec(cues[i].text + ' ' + (i + 1 < cues.length ? cues[i + 1].text : ''))) !== null) {
      // Only attribute to this cue if the match starts within this cue's text
      if (m.index >= cues[i].text.length) continue;
      const book = normalizeBook(m[1]);
      const chapter = +m[2], verse = +m[3], verseEnd = m[4] ? +m[4] : null;
      const refStr = `${book} ${chapter}:${verse}` + (verseEnd ? `-${verseEnd}` : '');
      // De-dupe identical refs within 60s
      const key = refStr + '|' + Math.floor(cues[i].start / 60);
      if (seen.has(key)) continue;
      seen.add(key);
      refs.push({ reference: refStr, book, chapter, verse, verseEnd, timestamp: Math.round(cues[i].start), spoken: m[0] });
    }
    // Spoken-number refs in this cue window ("Romans six three").
    for (const s of matchSpokenRefs(text)) {
      if (s.index >= cues[i].text.length) continue; // attribute to this cue only
      const key = s.reference + '|' + Math.floor(cues[i].start / 60);
      if (seen.has(key)) continue;
      seen.add(key);
      refs.push({ reference: s.reference, book: s.book, chapter: s.chapter, verse: s.verse, verseEnd: s.verseEnd, timestamp: Math.round(cues[i].start), spoken: s.spoken });
    }
  }
  refs.sort((a, b) => a.timestamp - b.timestamp);
  return refs;
}

// Get scripture references for one asset (with caching in server memory)
const _refsCache = {}; // assetId -> { refs, at }
app.get('/api/assets/:id/scripture-refs', async (req, res) => {
  try {
    const cached = _refsCache[req.params.id];
    if (cached && Date.now() - cached.at < 60 * 60 * 1000) return res.json({ status: 'ready', refs: cached.refs });
    const current = await mux('GET', `/video/v1/assets/${req.params.id}`);
    if (!current.data) return res.status(404).json({ error: 'Asset not found' });
    const refs = await extractScriptureRefs(current.data);
    if (refs === null) return res.json({ status: 'no-transcript', refs: [] });
    _refsCache[req.params.id] = { refs, at: Date.now() };
    res.json({ status: 'ready', refs });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Export ALL sermons' scripture references (for the Bible app)
// Returns [{ assetId, playbackId, title, date, refs: [{reference, book, chapter, verse, timestamp}] }]
app.get('/api/scripture-index', async (req, res) => {
  try {
    let cursor = null;
    const out = [];
    while (true) {
      const data = await mux('GET', cursor
        ? `/video/v1/assets?limit=100&cursor=${encodeURIComponent(cursor)}`
        : '/video/v1/assets?limit=100');
      for (const asset of data.data || []) {
        if (asset.status !== 'ready' || !isSermonAsset(asset)) continue;
        try {
          const cached = _refsCache[asset.id];
          const refs = (cached && Date.now() - cached.at < 60 * 60 * 1000)
            ? cached.refs
            : await extractScriptureRefs(asset);
          if (refs === null) continue;
          _refsCache[asset.id] = { refs, at: Date.now() };
          out.push({
            assetId: asset.id,
            playbackId: asset.playback_ids?.[0]?.id || null,
            title: asset.meta?.title || 'Untitled',
            date: asset.created_at ? new Date(asset.created_at * 1000).toISOString().slice(0, 10) : null,
            duration: asset.duration || null,
            refs,
          });
        } catch (e) {
          console.error(`[scripture-index] ${asset.id}: ${e.message}`);
        }
      }
      if (data.next_cursor) cursor = data.next_cursor; else break;
    }
    res.json({ sermons: out, generatedAt: new Date().toISOString() });
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

// Enable auto-generated LIVE captions on a sermon live stream (or any stream id).
// Must be idle (Mux constraint). POST /api/live-streams/:id/live-captions
// Body: { language_code?: "en" }. With no :id, enables on all sermon streams.
app.post('/api/live-streams/:id/live-captions', adminOnly, async (req, res) => {
  try {
    const r = await ensureLiveCaptions(req.params.id);
    res.json(r);
  } catch (e) {
    res.status(409).json({ error: e.message });
  }
});
app.post('/api/sermon-live-captions/enable-all', adminOnly, async (req, res) => {
  const results = [];
  for (const sid of SERMON_STREAM_IDS) {
    try { results.push({ streamId: sid, ...(await ensureLiveCaptions(sid)) }); }
    catch (e) { results.push({ streamId: sid, ok: false, error: e.message }); }
  }
  res.json({ results });
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
    const body = {
      playback_policy: ['public'],
      new_asset_settings: { playback_policy: ['public'], video_quality: 'plus' },
      meta: { title: resolvedTitle },
      passthrough,
      latency_mode: 'standard',
    };
    // Sermon streams get auto-generated English LIVE captions out of the gate.
    if (cat.toLowerCase().trim() === 'sermon') {
      body.generated_subtitles = [{ name: 'English (auto)', language_code: 'en' }];
    }
    const data = await mux('POST', '/video/v1/live-streams', body);
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

    // Resize to max 800px wide, compress.
    // PNGs keep PNG format to preserve transparency (round logos etc.); others → JPEG.
    const isPng = ext === 'png';
    const pipeline = sharp(req.file.buffer).resize({ width: 800, withoutEnlargement: true });
    const compressed = isPng
      ? await pipeline.png({ compressionLevel: 9 }).toBuffer()
      : await pipeline.jpeg({ quality: 80 }).toBuffer();

    await file.save(compressed, {
      metadata: { contentType: isPng ? 'image/png' : 'image/jpeg' },
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

    // Look up privateAccess + pendingApproval + pastorElder from Firestore users collection
    const privateMap = {};
    const pendingMap = {};
    const pastorElderMap = {};
    try {
      const privateSnap = await admin.firestore().collection('users').get();
      privateSnap.forEach(doc => {
        const d = doc.data();
        if (d.privateAccess) privateMap[doc.id] = true;
        if (d.pendingApproval) pendingMap[doc.id] = true;
        if (d.pastorElder) pastorElderMap[doc.id] = true;
      });
    } catch (e) {
      console.warn('Private access lookup failed:', e.message);
    }

    users.forEach(u => {
      u.platforms = platformsMap[u.uid] ? [...platformsMap[u.uid]] : [];
      u.minutesWatched = watchMap[u.uid] || 0;
      u.privateAccess = !!privateMap[u.uid];
      u.pendingApproval = !!pendingMap[u.uid];
      u.pastorElder = !!pastorElderMap[u.uid];
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

// Approve a pending registration (clears pendingApproval — app unlocks in real time)
// Pass {"approved": false} to revert a user back to pending (for testing).
app.patch('/api/users/:uid/approve', adminOnly, async (req, res) => {
  if (!sa) return res.status(503).json({ error: 'Firebase Admin not configured' });
  try {
    const approved = req.body && req.body.approved === false ? false : true;
    await admin.firestore().collection('users').doc(req.params.uid).set(
      { pendingApproval: !approved, approved, approvedAt: approved ? new Date().toISOString() : null },
      { merge: true }
    );
    res.json({ ok: true, uid: req.params.uid, approved });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// One-time backfill: pre-approve all existing users (except blocked/disabled ones)
app.post('/api/users/backfill-approve', adminOnly, async (req, res) => {
  if (!sa) return res.status(503).json({ error: 'Firebase Admin not configured' });
  try {
    const db = admin.firestore();
    let approved = 0, skippedBlocked = 0, pageToken;
    do {
      const page = await admin.auth().listUsers(1000, pageToken);
      for (const u of page.users) {
        if (u.disabled) { skippedBlocked++; continue; }
        // Double-check Firestore blocked flag
        const doc = await db.collection('users').doc(u.uid).get();
        if (doc.exists && doc.data().blocked === true) { skippedBlocked++; continue; }
        await db.collection('users').doc(u.uid).set(
          { pendingApproval: false, approved: true, approvedAt: new Date().toISOString(), approvedVia: 'backfill' },
          { merge: true }
        );
        // Stamp newSignups so the app never re-treats this user as a new registration
        await db.collection('newSignups').doc(u.uid).set(
          { uid: u.uid, email: u.email || '', displayName: u.displayName || '', notified: true, backfilled: true },
          { merge: true }
        );
        approved++;
      }
      pageToken = page.pageToken;
    } while (pageToken);
    res.json({ ok: true, approved, skippedBlocked });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Backfill displayName + email from Firebase Auth into every Firestore users
// doc, so the in-app pastor directory (which reads Firestore, not Auth) shows
// real names instead of "Unknown".
app.post('/api/users/backfill-names', adminOnly, async (req, res) => {
  if (!sa) return res.status(503).json({ error: 'Firebase Admin not configured' });
  try {
    const db = admin.firestore();
    let updated = 0, skipped = 0, pageToken;
    do {
      const page = await admin.auth().listUsers(1000, pageToken);
      for (const u of page.users) {
        const name = u.displayName || '';
        const email = u.email || '';
        if (!name && !email) { skipped++; continue; }
        await db.collection('users').doc(u.uid).set(
          { displayName: name, email: email },
          { merge: true }
        );
        updated++;
      }
      pageToken = page.pageToken;
    } while (pageToken);
    res.json({ ok: true, updated, skipped });
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

// Grant or revoke Pastor/Elder role (independent of admin/private access).
// Only affects who appears in the Pastor "My Highlights" share list.
app.patch('/api/users/:uid/pastor-elder', adminOnly, async (req, res) => {
  if (!sa) return res.status(503).json({ error: 'Firebase Admin not configured' });
  try {
    const { pastorElder } = req.body; // true = grant, false = revoke
    await admin.firestore().collection('users').doc(req.params.uid).set(
      { pastorElder: !!pastorElder },
      { merge: true }
    );
    res.json({ ok: true, uid: req.params.uid, pastorElder: !!pastorElder });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// List Pastor/Elder users (for the My Highlights share recipient picker in-app).
app.get('/api/pastor-elders', async (req, res) => {
  if (!sa) return res.status(503).json({ error: 'Firebase Admin not configured' });
  try {
    const snap = await admin.firestore().collection('users')
      .where('pastorElder', '==', true).get();
    const ids = [];
    snap.forEach(doc => ids.push(doc.id));
    const out = [];
    for (const uid of ids) {
      try {
        const u = await admin.auth().getUser(uid);
        out.push({ uid, email: u.email || '', displayName: u.displayName || '' });
      } catch (_) { out.push({ uid, email: '', displayName: '' }); }
    }
    out.sort((a, b) => (a.displayName || a.email).localeCompare(b.displayName || b.email));
    res.json({ pastorElders: out });
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
    const { title, author, description, coverImageUrl, featuredImageUrl, category, amazonUrl, kindleUrl, audiobookUrl, coverFit, featured, sortOrder } = req.body;
    if (!title) return res.status(400).json({ error: 'title required' });
    const data = {
      title,
      author: author || '',
      description: description || '',
      coverImageUrl: coverImageUrl || '',
      featuredImageUrl: featuredImageUrl || '',
      category: category || '',
      amazonUrl: amazonUrl || '',
      kindleUrl: kindleUrl || '',
      audiobookUrl: audiobookUrl || '',
      coverFit: (coverFit === 'fit') ? 'fit' : 'fill',
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
    const { title, author, description, coverImageUrl, featuredImageUrl, category, amazonUrl, kindleUrl, audiobookUrl, coverFit, featured, sortOrder } = req.body;
    const update = { updatedAt: admin.firestore.FieldValue.serverTimestamp() };
    if (title !== undefined) update.title = title;
    if (author !== undefined) update.author = author;
    if (description !== undefined) update.description = description;
    if (coverImageUrl !== undefined) update.coverImageUrl = coverImageUrl;
    if (featuredImageUrl !== undefined) update.featuredImageUrl = featuredImageUrl;
    if (category !== undefined) update.category = category;
    if (amazonUrl !== undefined) update.amazonUrl = amazonUrl;
    if (kindleUrl !== undefined) update.kindleUrl = kindleUrl;
    if (audiobookUrl !== undefined) update.audiobookUrl = audiobookUrl;
    if (coverFit !== undefined) update.coverFit = (coverFit === 'fit') ? 'fit' : 'fill';
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

app.delete('/api/music/album/:albumId', editorOrAdmin, async (req, res) => {
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

app.delete('/api/music/playlist/:playlistId', editorOrAdmin, async (req, res) => {
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
    const { title, author, content, excerpt, coverImageUrl, featuredImageUrl, category, published, featured, sortOrder, pdfUrl } = req.body;
    if (!title) return res.status(400).json({ error: 'title required' });
    const data = {
      title,
      author: author || '',
      content: content || '',
      excerpt: excerpt || '',
      coverImageUrl: coverImageUrl || '',
      featuredImageUrl: featuredImageUrl || '',
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
    const { title, author, content, excerpt, coverImageUrl, featuredImageUrl, category, published, featured, sortOrder, pdfUrl } = req.body;
    const update = { updatedAt: admin.firestore.FieldValue.serverTimestamp() };
    if (title !== undefined) update.title = title;
    if (author !== undefined) update.author = author;
    if (content !== undefined) update.content = content;
    if (excerpt !== undefined) update.excerpt = excerpt;
    if (coverImageUrl !== undefined) update.coverImageUrl = coverImageUrl;
    if (featuredImageUrl !== undefined) update.featuredImageUrl = featuredImageUrl;
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
    const { title, feedUrl, description, artworkUrl, featuredImageUrl, category, enabled, featured, sortOrder } = req.body;
    if (!title) return res.status(400).json({ error: 'title required' });
    if (!feedUrl) return res.status(400).json({ error: 'feedUrl required' });
    const data = {
      title,
      feedUrl,
      description: description || '',
      artworkUrl: artworkUrl || '',
      featuredImageUrl: featuredImageUrl || '',
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
    const { title, feedUrl, description, artworkUrl, featuredImageUrl, category, enabled, featured, sortOrder } = req.body;
    const update = {};
    if (title !== undefined) update.title = title;
    if (feedUrl !== undefined) update.feedUrl = feedUrl;
    if (description !== undefined) update.description = description;
    if (artworkUrl !== undefined) update.artworkUrl = artworkUrl;
    if (featuredImageUrl !== undefined) update.featuredImageUrl = featuredImageUrl;
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
    const { title, artist, description, audioUrl, coverImageUrl, featuredImageUrl, category, duration, featured, sortOrder, seriesId, episodeNumber, mediaType } = req.body;
    if (!title) return res.status(400).json({ error: 'title required' });
    const data = {
      title,
      artist: artist || '',
      description: description || '',
      audioUrl: audioUrl || '',
      coverImageUrl: coverImageUrl || '',
      featuredImageUrl: featuredImageUrl || '',
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
    const { title, artist, description, audioUrl, coverImageUrl, featuredImageUrl, category, duration, featured, sortOrder, seriesId, episodeNumber, mediaType } = req.body;
    const update = {};
    if (title !== undefined) update.title = title;
    if (artist !== undefined) update.artist = artist;
    if (description !== undefined) update.description = description;
    if (audioUrl !== undefined) update.audioUrl = audioUrl;
    if (coverImageUrl !== undefined) update.coverImageUrl = coverImageUrl;
    if (featuredImageUrl !== undefined) update.featuredImageUrl = featuredImageUrl;
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
    const { title, description, artworkUrl, featuredImageUrl, category, mediaType, enabled, featured, sortOrder } = req.body;
    if (!title) return res.status(400).json({ error: 'title required' });
    const data = {
      title,
      description: description || '',
      artworkUrl: artworkUrl || '',
      featuredImageUrl: featuredImageUrl || '',
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
    const { title, description, artworkUrl, featuredImageUrl, category, mediaType, enabled, featured, sortOrder } = req.body;
    const update = {};
    if (title !== undefined) update.title = title;
    if (description !== undefined) update.description = description;
    if (artworkUrl !== undefined) update.artworkUrl = artworkUrl;
    if (featuredImageUrl !== undefined) update.featuredImageUrl = featuredImageUrl;
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

// Mark a whole series as Pastor Content (private): sets the series category to
// 'hidden' AND cascades category='hidden' to every audio file in that series.
// Body: { category: 'hidden' } (default) or { category: '<other>' } to move the
// whole series + its files to another category.
app.patch('/api/series/:id/pastor', async (req, res) => {
  try {
    const db = admin.firestore();
    const category = (req.body && req.body.category) || 'hidden';
    const seriesRef = db.collection('series').doc(req.params.id);
    const epSnap = await db.collection('audioAssets')
      .where('seriesId', '==', req.params.id).get();
    // Firestore batches cap at 500 writes; chunk to be safe.
    const docs = [seriesRef, ...epSnap.docs.map(d => d.ref)];
    for (let i = 0; i < docs.length; i += 450) {
      const batch = db.batch();
      for (const ref of docs.slice(i, i + 450)) batch.update(ref, { category });
      await batch.commit();
    }
    console.log(`[series/pastor] series ${req.params.id} + ${epSnap.size} episodes -> category=${category}`);
    res.json({ ok: true, episodesUpdated: epSnap.size, category });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Portal Version ─────────────────────────────
// PORTAL_BUILD = git commit count at deploy time. Bump alongside each deploy commit.
const PORTAL_BUILD = 245;
app.get('/api/version', (req, res) => {
  res.json({
    build: PORTAL_BUILD,
    commit: process.env.RAILWAY_GIT_COMMIT_SHA ? process.env.RAILWAY_GIT_COMMIT_SHA.slice(0, 7) : null,
    startedAt: serverStartedAt
  });
});
const serverStartedAt = new Date().toISOString();

// ── Featured Videos Config ─────────────────────────────
// Lists stored in config/featured: ids (main carousel) + per-group rows
const FEATURED_GROUP_FIELDS = ['ids', 'parents', 'youngPeople', 'children'];

app.get('/api/config/featured', async (req, res) => {
  try {
    const db = admin.firestore();
    const doc = await db.collection('config').doc('featured').get();
    const data = doc.exists ? (doc.data() || {}) : {};
    const out = {};
    for (const f of FEATURED_GROUP_FIELDS) out[f] = Array.isArray(data[f]) ? data[f] : [];
    res.json(out);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.put('/api/config/featured', async (req, res) => {
  try {
    const db = admin.firestore();
    const update = {};
    for (const f of FEATURED_GROUP_FIELDS) {
      if (req.body[f] !== undefined) {
        if (!Array.isArray(req.body[f])) return res.status(400).json({ error: `${f} must be an array` });
        update[f] = req.body[f];
      }
    }
    if (Object.keys(update).length === 0) return res.status(400).json({ error: 'no valid list fields provided' });
    // merge:true so updating one list never wipes the others
    await db.collection('config').doc('featured').set(update, { merge: true });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.listen(PORT, async () => {
  console.log(`GO Admin running on :${PORT}`);
  if (sa) await loadPortalEditors();
});
// Railway redeploy trigger 1780454656
// deploy 1780466198
