#!/usr/bin/env node
/**
 * Local sermon thumbnail generator.
 * Runs on this Mac (where fonts work) to generate + upload dated sermon
 * thumbnails for any sermon asset that needs one.
 *
 * Usage:
 *   node scripts/generate-sermon-thumbs.js          # skip assets with existing thumbs
 *   node scripts/generate-sermon-thumbs.js --force  # regenerate all
 *
 * Required env: FIREBASE_SA (JSON service account key)
 * Reads from go-admin API (admin:gomedia) to find sermon assets.
 */

process.chdir(require('path').resolve(__dirname, '..'));
const { generateSermonThumbnail, formatSermonDate } = require('./services/sermonThumbnail');
const admin = require('firebase-admin');
const fs = require('fs');

const force = process.argv.includes('--force');

const sa = JSON.parse(process.env.FIREBASE_SA || 'null');
if (!sa) { console.error('FIREBASE_SA not set'); process.exit(1); }
admin.initializeApp({ credential: admin.credential.cert(sa), storageBucket: 'gospel-outreach-tv.firebasestorage.app' });

const MUX_TOKEN_ID = process.env.MUX_TOKEN_ID || '25cd1f0d-e6d4-445b-a106-e9ccc7a9f103';
const MUX_SECRET   = process.env.MUX_SECRET   || 'AcQYv3xI4uyhDIOmgAfaP+rAX9ei6bXzpT95dcAc74ALgOAl04BLg6o9PYwGh/iljlF4FTYz2VM';
const MUX_AUTH = Buffer.from(`${MUX_TOKEN_ID}:${MUX_SECRET}`).toString('base64');

async function mux(method, path, body) {
  const res = await fetch(`https://api.mux.com${path}`, {
    method,
    headers: { Authorization: `Basic ${MUX_AUTH}`, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  return res.json();
}

async function run() {
  const bucket = admin.storage().bucket();

  // Fetch all assets
  const assetsRes = await fetch('https://go-admin-production-6be4.up.railway.app/api/assets', {
    headers: { 'Authorization': 'Basic ' + Buffer.from('admin:gomedia').toString('base64') }
  });
  const { data: allAssets } = await assetsRes.json();
  const sermons = allAssets.filter(a => {
    try {
      const pt = JSON.parse(a.passthrough || '{}');
      return (pt.category || '').toLowerCase() === 'sermon' ||
             a.live_stream_id === 'ECgSydhoD601OoMqVmiNvfH6y0100m8uENxk6KKJV4NSZQ';
    } catch { return false; }
  });

  console.log(`Found ${sermons.length} sermon assets (force=${force})`);
  let processed = 0, skipped = 0;

  for (const asset of sermons) {
    let pt;
    try { pt = JSON.parse(asset.passthrough || '{}'); } catch { pt = {}; }

    if (pt.thumbnail && !force) {
      skipped++;
      continue;
    }

    const dateStr = formatSermonDate(asset.created_at);
    const isoDate = new Date(asset.created_at * 1000).toISOString().slice(0, 10);

    // Generate
    const buf = await generateSermonThumbnail(dateStr);

    // Upload to Firebase Storage
    const filename = `sermon-thumbs/sermon_${isoDate}.jpg`;
    const file = bucket.file(filename);
    await file.save(buf, { metadata: { contentType: 'image/jpeg' } });
    await file.makePublic();
    const url = `https://storage.googleapis.com/${bucket.name}/${filename}`;

    // Update Mux passthrough
    pt.thumbnail = url;
    await mux('PATCH', `/video/v1/assets/${asset.id}`, { passthrough: JSON.stringify(pt) });

    console.log(`✅ ${asset.id} (${dateStr}): ${url}`);
    processed++;
    await new Promise(r => setTimeout(r, 300));
  }

  console.log(`Done: processed=${processed} skipped=${skipped}`);
  process.exit(0);
}
run().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
