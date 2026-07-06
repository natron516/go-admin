/**
 * Sermon Thumbnail Service
 *
 * Composites a dated 16:9 branded thumbnail (1920×1080) for every sermon
 * live-stream VOD. The base template (assets/sermon_thumb_base.jpg) has the
 * cross emblem and title text baked in; we overlay the date in large
 * metallic-cream serif text matching the approved design reference.
 *
 * Font notes: Railway's minimal Linux image has no system fonts, so
 * we embed CrimsonText Bold (assets/sermon_font_bold.ttf) as a base64 data URI
 * in the SVG <style> block. This guarantees rendering on any OS.
 *
 * Exported helpers:
 *   generateSermonThumbnail(dateStr)  -> Buffer (JPEG)
 *   generateAndUploadSermonThumb(dateStr, label, { bucket }) -> { url }
 */

const path = require('path');
const fs = require('fs');
const sharp = require('sharp');

// Base template: 1376×768 (what the AI generated), but we compose at 1920×1080
const BASE_IMG = path.join(__dirname, '..', 'assets', 'sermon_thumb_base.jpg');
// Embedded serif font: CrimsonText Bold. Loaded once at startup and cached.
const FONT_PATH = path.join(__dirname, '..', 'assets', 'sermon_font_bold.ttf');
let _fontB64 = null;
function getFontB64() {
  if (!_fontB64) _fontB64 = fs.readFileSync(FONT_PATH).toString('base64');
  return _fontB64;
}

const OUT_W = 1920;
const OUT_H = 1080;

// Positioning: right panel center x=1423, date at y=580 (~54% from top).
// Kept well above the bottom 20% (216px) so the app's transcript badge doesn't
// overlap. Font size 88px with embedded CrimsonText Bold serif.
const DATE_X = 1423;
const DATE_Y = 580;   // was 688 (too low); moved up to 580
const FONT_SIZE = 88;

const MONTHS = [
  'JANUARY','FEBRUARY','MARCH','APRIL','MAY','JUNE',
  'JULY','AUGUST','SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER',
];

/**
 * Format a JS Date (or unix epoch seconds) as "JULY 5, 2026".
 * Uses Pacific Time (UTC-7 PDT / UTC-8 PST) offset manually so this works on
 * minimal Node/ICU builds (Railway's image may lack full timezone data).
 * @param {Date|number} date
 */
function formatSermonDate(date) {
  // Accept: number (unix seconds), numeric string (unix seconds), or Date
  let ms;
  if (date instanceof Date) {
    ms = date.getTime();
  } else {
    const n = Number(date);
    // If it parses as a finite number, treat as unix seconds
    ms = isFinite(n) ? n * 1000 : NaN;
  }
  if (!isFinite(ms)) return 'SERMON DATE';
  // Determine Pacific offset: PDT = UTC-7 (second Sun Mar - first Sun Nov), else PST = UTC-8
  // Simple approximation: check if the UTC date falls within PDT window
  const utc = new Date(ms);
  const year = utc.getUTCFullYear();
  // DST starts: 2nd Sunday in March; ends: 1st Sunday in November
  const dstStart = new Date(Date.UTC(year, 2, 1));
  dstStart.setUTCDate(8 - ((dstStart.getUTCDay() + 6) % 7)); // 2nd Sunday
  dstStart.setUTCDate(dstStart.getUTCDate() + 7);
  dstStart.setUTCHours(10, 0, 0, 0); // 2:00 AM PST = 10:00 UTC
  const dstEnd = new Date(Date.UTC(year, 10, 1));
  dstEnd.setUTCDate(1 + (7 - dstEnd.getUTCDay()) % 7); // 1st Sunday
  dstEnd.setUTCHours(9, 0, 0, 0); // 2:00 AM PDT = 09:00 UTC
  const offsetMs = (ms >= dstStart.getTime() && ms < dstEnd.getTime()) ? -7 * 3600000 : -8 * 3600000;
  const local = new Date(ms + offsetMs);
  const month = MONTHS[local.getUTCMonth()];
  const day = local.getUTCDate();
  const yr = local.getUTCFullYear();
  return `${month} ${day}, ${yr}`;
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

  const fontB64 = getFontB64();

  const svg = `<svg width="${OUT_W}" height="${OUT_H}" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <style>
      @font-face {
        font-family: 'CrimsonText';
        font-weight: bold;
        src: url('data:font/truetype;base64,${fontB64}') format('truetype');
      }
    </style>
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
    font-family="CrimsonText, Georgia, serif"
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
