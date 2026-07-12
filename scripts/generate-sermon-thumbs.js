#!/usr/bin/env node
/**
 * Local sermon thumbnail generator.
 * Runs on this Mac (where fonts work) to generate + upload dated sermon
 * thumbnails. Does TWO things every Sunday:
 *
 *   1. EXISTING assets: backfill any sermon asset missing a pt.thumbnail.
 *   2. FUTURE Sundays: pre-generate thumbnails for the next N Sundays
 *      (default 4) and upload to Firebase Storage as sermon_YYYY-MM-DD.jpg.
 *      The server-side webhook reads these at stream-end and sets pt.thumbnail
 *      immediately — no cron delay.
 *
 * Usage:
 *   node scripts/generate-sermon-thumbs.js                  # normal run
 *   node scripts/generate-sermon-thumbs.js --force          # regenerate all existing
 *   node scripts/generate-sermon-thumbs.js --future-weeks=8 # pre-generate 8 weeks ahead
 *
 * Required env: FIREBASE_SA (JSON service account key)
 * Reads from go-admin API (admin:gomedia) to find sermon assets.
 */

process.chdir(require('path').resolve(__dirname, '..'));
const { generateSermonThumbnail, formatSermonDate } = require('../services/sermonThumbnail');
const admin = require('firebase-admin');

const force = process.argv.includes('--force');
const futureWeeksArg = process.argv.find(a => a.startsWith('--future-weeks='));
const FUTURE_WEEKS = futureWeeksArg ? parseInt(futureWeeksArg.split('=')[1], 10) : 4;

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

/** Get the next N Sunday dates (Pacific time) starting from today or next Sunday. */
function getUpcomingSundays(n, includeToday = true) {
  const now = new Date();
  // Work in Pacific time: offset in ms
  const isPDT = (() => {
    const y = now.getUTCFullYear();
    const dstStart = new Date(Date.UTC(y, 2, 1));
    dstStart.setUTCDate(8 - ((dstStart.getUTCDay() + 6) % 7));
    dstStart.setUTCDate(dstStart.getUTCDate() + 7);
    dstStart.setUTCHours(10, 0, 0, 0);
    const dstEnd = new Date(Date.UTC(y, 10, 1));
    dstEnd.setUTCDate(1 + (7 - dstEnd.getUTCDay()) % 7);
    dstEnd.setUTCHours(9, 0, 0, 0);
    return now >= dstStart && now < dstEnd;
  })();
  const offsetMs = isPDT ? -7 * 3600000 : -8 * 3600000;
  const localNow = new Date(now.getTime() + offsetMs);
  const dayOfWeek = localNow.getUTCDay(); // 0=Sun
  // Days until next Sunday (0 if today is Sunday)
  const daysUntilSunday = includeToday ? ((7 - dayOfWeek) % 7) : (7 - dayOfWeek) || 7;
  const sundays = [];
  for (let i = 0; i < n; i++) {
    const d = new Date(localNow.getTime() + (daysUntilSunday + i * 7) * 86400000);
    // Return as ISO date string (YYYY-MM-DD) in Pacific time
    const iso = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2,'0')}-${String(d.getUTCDate()).padStart(2,'0')}`;
    sundays.push(iso);
  }
  return sundays;
}

/** formatSermonDate from an ISO date string (YYYY-MM-DD) for display text on the thumbnail. */
function formatDateStr(isoDate) {
  // isoDate is already Pacific-local; just parse it for display
  const [y, m, d] = isoDate.split('-').map(Number);
  const MONTHS = ['January','February','March','April','May','June','July','August','September','October','November','December'];
  return `${MONTHS[m - 1]} ${d}, ${y}`;
}

async function run() {
  const bucket = admin.storage().bucket();

  // ── Part 1: Backfill existing sermon assets ──────────────────────────────
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
    const baseUrl = `https://storage.googleapis.com/${bucket.name}/${filename}`;

    // Cache-bust if overwriting existing URL
    let url = baseUrl;
    if (force && pt.thumbnail && pt.thumbnail.startsWith(baseUrl)) {
      const vMatch = pt.thumbnail.match(/\?v=(\d+)$/);
      url = `${baseUrl}?v=${vMatch ? parseInt(vMatch[1], 10) + 1 : 2}`;
    }

    // Update Mux passthrough
    pt.thumbnail = url;
    await mux('PATCH', `/video/v1/assets/${asset.id}`, { passthrough: JSON.stringify(pt) });

    console.log(`✅ ${asset.id} (${dateStr}): ${url}`);
    processed++;
    await new Promise(r => setTimeout(r, 300));
  }

  console.log(`Backfill done: processed=${processed} skipped=${skipped}`);

  // ── Part 2: Pre-generate thumbnails for upcoming Sundays ────────────────
  // Upload to Storage so the server-side webhook can find them at stream-end.
  // Skips dates that already have a file in Storage (unless --force).
  console.log(`\nPre-generating thumbnails for next ${FUTURE_WEEKS} Sundays...`);
  const upcomingSundays = getUpcomingSundays(FUTURE_WEEKS, false); // exclude today (just streamed)
  let futureProcessed = 0, futureSkipped = 0;

  for (const isoDate of upcomingSundays) {
    const filename = `sermon-thumbs/sermon_${isoDate}.jpg`;
    const file = bucket.file(filename);
    if (!force) {
      const [exists] = await file.exists();
      if (exists) {
        console.log(`⏭ ${isoDate}: already in Storage — skip`);
        futureSkipped++;
        continue;
      }
    }
    const dateStr = formatDateStr(isoDate);
    const buf = await generateSermonThumbnail(dateStr);
    await file.save(buf, { metadata: { contentType: 'image/jpeg' } });
    await file.makePublic();
    const url = `https://storage.googleapis.com/${bucket.name}/${filename}`;
    console.log(`🗓 ${isoDate} (${dateStr}): pre-generated → ${url}`);
    futureProcessed++;
    await new Promise(r => setTimeout(r, 300));
  }

  console.log(`Future pre-gen done: generated=${futureProcessed} skipped=${futureSkipped}`);
  process.exit(0);
}
run().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
