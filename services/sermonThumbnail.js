/**
 * Sermon Thumbnail Service
 *
 * Composites a dated 16:9 branded thumbnail (1920×1080) for every sermon
 * live-stream VOD. The base template (assets/sermon_thumb_base.jpg) has the
 * cross emblem and title text baked in; we overlay the date in large
 * metallic-cream serif text matching the approved design reference.
 *
 * Exported helpers:
 *   generateSermonThumbnail(dateStr)  -> Buffer (JPEG)
 *   generateAndUploadSermonThumb(dateStr, label, { bucket }) -> { url }
 */

const path = require('path');
const sharp = require('sharp');

// Base template: 1376×768 (what the AI generated), but we compose at 1920×1080
const BASE_IMG = path.join(__dirname, '..', 'assets', 'sermon_thumb_base.jpg');

const OUT_W = 1920;
const OUT_H = 1080;

// Empirically-tuned from the approved reference (see memory/2026-07-06.md):
// - Right panel center x ≈ 1020/1376 * 1920 = 1423
// - Date vertical center ≈ 490/768  * 1080 = 688
// - Font size 88px at 1920 wide (fits with comfortable margin)
const DATE_X = Math.round((1020 / 1376) * OUT_W); // 1423
const DATE_Y = Math.round((490 / 768)  * OUT_H);  // 688
const FONT_SIZE = 88;

/**
 * Format a JS Date (or unix epoch seconds) as "JULY 5, 2026".
 * @param {Date|number} date
 */
function formatSermonDate(date) {
  const d = typeof date === 'number' ? new Date(date * 1000) : new Date(date);
  return d.toLocaleDateString('en-US', {
    timeZone: 'America/Los_Angeles',
    month: 'long',
    day: 'numeric',
    year: 'numeric',
  }).toUpperCase(); // "JULY 5, 2026"
}

/**
 * Generate a sermon thumbnail JPEG buffer for the given date string.
 * @param {string} dateStr  e.g. "JULY 5, 2026"
 * @returns {Promise<Buffer>}
 */
async function generateSermonThumbnail(dateStr) {
  const escaped = String(dateStr)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

  const svg = `<svg width="${OUT_W}" height="${OUT_H}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="metallic" x1="0%" y1="0%" x2="0%" y2="100%">
      <stop offset="0%"   stop-color="#f2e8cc"/>
      <stop offset="25%"  stop-color="#e8dcb8"/>
      <stop offset="50%"  stop-color="#d8cca0"/>
      <stop offset="75%"  stop-color="#c4b484"/>
      <stop offset="100%" stop-color="#b0a070"/>
    </linearGradient>
    <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
      <feDropShadow dx="0" dy="4" stdDeviation="5" flood-color="rgba(0,0,0,0.65)"/>
    </filter>
  </defs>
  <text
    x="${DATE_X}"
    y="${DATE_Y}"
    text-anchor="middle"
    font-family="Georgia, 'Times New Roman', serif"
    font-size="${FONT_SIZE}"
    font-weight="bold"
    letter-spacing="3"
    fill="url(#metallic)"
    filter="url(#shadow)"
  >${escaped}</text>
</svg>`;

  return sharp(BASE_IMG)
    .resize(OUT_W, OUT_H, { fit: 'fill' })
    .composite([{ input: Buffer.from(svg), top: 0, left: 0 }])
    .jpeg({ quality: 90 })
    .toBuffer();
}

/**
 * Generate a thumbnail and upload it to Firebase Storage.
 * Returns the public URL.
 *
 * @param {string} dateStr     e.g. "JULY 5, 2026"
 * @param {string} label       used in the storage filename, e.g. "2026-07-05"
 * @param {{ bucket: object }} opts  firebase-admin storage bucket
 * @returns {Promise<{ url: string }>}
 */
async function generateAndUploadSermonThumb(dateStr, label, { bucket }) {
  const buf = await generateSermonThumbnail(dateStr);
  const slug = label.replace(/[^a-z0-9\-]/gi, '-').toLowerCase();
  const filename = `sermon-thumbs/sermon_${slug}.jpg`;
  const file = bucket.file(filename);
  await file.save(buf, { metadata: { contentType: 'image/jpeg' } });
  await file.makePublic();
  const url = `https://storage.googleapis.com/${bucket.name}/${filename}`;
  return { url };
}

module.exports = { generateSermonThumbnail, generateAndUploadSermonThumb, formatSermonDate };
