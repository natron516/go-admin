/**
 * Sermon Thumbnail Service
 *
 * Composites a dated 16:9 branded thumbnail (1920×1080) for every sermon
 * live-stream VOD. The base template (assets/sermon_thumb_base.jpg) has the
 * cross emblem and title text baked in; we overlay the date in large
 * metallic-cream serif text matching the approved design reference.
 *
 * Font strategy: use sharp's text input (Pango/FreeType path) with our bundled
 * CrimsonText Bold TTF specified via fontfile. This bypasses librsvg entirely
 * and works on minimal Linux containers where SVG font-face (data: or file:)
 * silently fails in librsvg.
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
// CrimsonText Bold serif font — used via sharp text input (Pango/FreeType).
// Bypasses librsvg which can't load custom fonts in minimal Linux containers.
const FONT_PATH = path.resolve(__dirname, '..', 'assets', 'sermon_font_bold.ttf');
// Note: FONTCONFIG_FILE is set in server.js BEFORE sharp is required,
// so fontconfig finds this TTF when libvips initialises.
console.log('[sermon-thumb] Font path:', FONT_PATH, 'exists:', fs.existsSync(FONT_PATH));

const OUT_W = 1920;
const OUT_H = 1080;

// Positioning: right panel center x=1423, date top-of-text at y=515 (~48% from top).
// Kept well above the bottom 20% (y>864) so the app's transcript badge doesn't
// overlap. Font renders at ~88-100px equivalent via Pango.
const DATE_CENTER_X = 1423;   // horizontal center of right panel
const DATE_TOP_Y = 515;       // top edge of text block
const DATE_TEXT_W = 960;      // Pango layout width (it will wrap if needed; wide enough for any date)
const DATE_TEXT_H = 120;      // height budget for text layer

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
 *
 * Uses sharp's text input (Pango/FreeType) to render the date glyph — this
 * works reliably on Railway Linux because it bypasses librsvg entirely.
 * Two-layer approach:
 *   1. Shadow layer (dark, offset) rendered first
 *   2. Main cream-coloured text layer on top
 *
 * @param {string} dateStr  e.g. "JULY 5, 2026"
 * @returns {Promise<Buffer>}
 */
async function generateSermonThumbnail(dateStr) {
  const safe = String(dateStr).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

  // Render shadow layer: dark offset text (for drop-shadow effect)
  const shadowBuf = await sharp({
    text: {
      text: `<span foreground="#111111">${safe}</span>`,
      font: 'CrimsonText Bold',
      fontfile: FONT_PATH,
      width: DATE_TEXT_W,
      height: DATE_TEXT_H,
      rgba: true,
      align: 'centre',
    }
  }).png().toBuffer();

  const shadowMeta = await sharp(shadowBuf).metadata();
  const tw = shadowMeta.width || DATE_TEXT_W;
  const th = shadowMeta.height || DATE_TEXT_H;

  // Render main cream text layer
  const textBuf = await sharp({
    text: {
      text: `<span foreground="#e8dcb8">${safe}</span>`,
      font: 'CrimsonText Bold',
      fontfile: FONT_PATH,
      width: DATE_TEXT_W,
      height: DATE_TEXT_H,
      rgba: true,
      align: 'centre',
    }
  }).png().toBuffer();

  // Center in right panel: left = DATE_CENTER_X - textWidth/2
  const left = Math.max(680, DATE_CENTER_X - Math.floor(tw / 2));
  const top = DATE_TOP_Y;
  const shadowLeft = Math.max(680, left + 3);
  const shadowTop = top + 5;

  return sharp(BASE_IMG)
    .resize(OUT_W, OUT_H, { fit: 'fill' })
    .composite([
      { input: shadowBuf, left: shadowLeft, top: shadowTop },
      { input: textBuf,   left: left,       top: top },
    ])
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
