const express = require('express');
const basicAuth = require('express-basic-auth');

const app = express();
const PORT = process.env.PORT || 3000;

const MUX_TOKEN_ID  = process.env.MUX_TOKEN_ID  || '25cd1f0d-e6d4-445b-a106-e9ccc7a9f103';
const MUX_SECRET    = process.env.MUX_SECRET     || 'AcQYv3xI4uyhDIOmgAfaP+rAX9ei6bXzpT95dcAc74ALgOAl04BLg6o9PYwGh/iljlF4FTYz2VM';
const ADMIN_USER    = process.env.ADMIN_USER     || 'admin';
const ADMIN_PASS    = process.env.ADMIN_PASS     || 'govideos2024';

const MUX_AUTH = Buffer.from(`${MUX_TOKEN_ID}:${MUX_SECRET}`).toString('base64');

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
    const { title, category, date, speaker } = req.body;
    const passthrough = JSON.stringify({
      category: category || 'sermon',
      title: title || 'Untitled',
      ...(date    && { date }),
      ...(speaker && { speaker }),
    });
    const data = await mux('POST', '/video/v1/uploads', {
      cors_origin: '*',
      new_asset_settings: {
        playback_policy: ['public'],
        meta: { title: title || 'Untitled' },
        passthrough,
      },
    });
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Update asset metadata (title / passthrough)
app.patch('/api/assets/:id', async (req, res) => {
  try {
    const { title, category, date, speaker } = req.body;
    const passthrough = JSON.stringify({
      category: category || 'sermon',
      title: title || 'Untitled',
      ...(date    && { date }),
      ...(speaker && { speaker }),
    });
    const data = await mux('PATCH', `/video/v1/assets/${req.params.id}`, {
      meta: { title: title || 'Untitled' },
      passthrough,
    });
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
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
    const passthrough = JSON.stringify({ category: category || 'sermon', title: title || 'Live Service' });
    const data = await mux('POST', '/video/v1/live-streams', {
      playback_policy: ['public'],
      new_asset_settings: { playback_policy: ['public'], passthrough },
      meta: { title: title || 'Live Service' },
      passthrough,
      latency_mode: 'standard',
    });
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () => console.log(`GO Admin running on :${PORT}`));
