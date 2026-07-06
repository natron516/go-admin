/**
 * Sermon Thumbnail Service
 *
 * Composites a dated 16:9 branded thumbnail (1920×1080) for every sermon
 * live-stream VOD. The base template (assets/sermon_thumb_base.jpg) has the
 * cross emblem and title text baked in; we overlay the date in Cinzel Bold
 * (a free Trajan-style serif) with a warm champagne-gold gradient, matching
 * the approved Isaac reference design.
 *
 * Text rendering: uses ImageMagick (`magick` CLI) for the text layers because
 * it reliably loads custom TTF files without fontconfig/librsvg issues. Falls
 * back to sharp-only (sans-serif) if magick is unavailable.
 *
 * Font: Cinzel Bold (assets/cinzel_bold.ttf) — free Trajan-style from Google Fonts
 *
 * Exported helpers:
 *   generateSermonThumbnail(dateStr)  -> Buffer (JPEG)
 *   generateAndUploadSermonThumb(dateStr, label, { bucket }) -> { url }
 */

const path = require('path');
const fs = require('fs');
const { execSync } = require('child_process');
const sharp = require('sharp');

// Base template: 1376×768 (what the AI generated), but we compose at 1920×1080
const BASE_IMG = path.join(__dirname, '..', 'assets', 'sermon_thumb_base.jpg');
// Cinzel Bold: free Trajan-style serif from Google Fonts.
const FONT_PATH = path.resolve(__dirname, '..', 'assets', 'cinzel_bold.ttf');

// Detect ImageMagick availability (used for color-accurate text rendering)
function getIMBin() {
  for (const bin of ['/opt/homebrew/bin/magick', '/usr/local/bin/magick', '/usr/bin/magick', 'magick']) {
    try { execSync(`${bin} --version`, { stdio: 'ignore' }); return bin; } catch(e) {}
  }
  return null;
}
const IM_BIN = getIMBin();
console.log('[sermon-thumb] Font:', FONT_PATH, 'exists:', fs.existsSync(FONT_PATH), '| ImageMagick:', IM_BIN || 'NOT FOUND');

const OUT_W = 1920;
const OUT_H = 1080;

// Positioning: right panel center x=1423, date top-of-text at y=508 (~47% from top).
// Kept well above the bottom 20% (y>864) so the app's transcript badge doesn't overlap.
const DATE_CENTER_X = 1423;   // horizontal center of right panel
const DATE_TOP_Y = 508;       // top edge of text block
const DATE_TEXT_W = 960;      // text layer width
const DATE_TEXT_H = 140;      // text layer height

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

// Temp file helpers (unique per call to avoid race conditions)
function tmpFile(suffix) {
  return path.join(require('os').tmpdir(), `sermon_thumb_${Date.now()}_${Math.random().toString(36).slice(2)}${suffix}`);
}

/**
 * Generate a sermon thumbnail JPEG buffer for the given date string.
 *
 * Primary path: ImageMagick renders Cinzel Bold text layers (shadow + warm gold
 * gradient), then sharp composites them onto the base image.
 *
 * Fallback: if ImageMagick is not available, falls back to a plain sharp text
 * overlay (may render sans-serif on systems without fontconfig).
 *
 * @param {string} dateStr  e.g. "JULY 5, 2026"
 * @returns {Promise<Buffer>}
 */
async function generateSermonThumbnail(dateStr) {
  if (IM_BIN) {
    return generateWithImageMagick(dateStr);
  }
  return generateFallback(dateStr);
}

/**
 * Primary path: ImageMagick + sharp composite.
 * Renders Cinzel Bold with warm champagne-gold gradient matching the reference design.
 */
async function generateWithImageMagick(dateStr) {
  // Escape single quotes for shell
  const escaped = String(dateStr).replace(/'/g, "'\\''" );
  const fontSize = 105;
  const W = 960, H = 140;
  const top = DATE_TOP_Y;
  const centerX = DATE_CENTER_X;
  
  const tShadow = tmpFile('_shadow.png');
  const tTop    = tmpFile('_top.png');
  const tBot    = tmpFile('_bot.png');
  const tVmask  = tmpFile('_vmask.png');
  const tGold   = tmpFile('_gold.png');
  const temps   = [tShadow, tTop, tBot, tVmask, tGold];
  
  const cleanup = () => temps.forEach(f => { try { fs.unlinkSync(f); } catch(e) {} });

  try {
    const IM = IM_BIN;
    const FONT = FONT_PATH;

    // Shadow layer (semi-transparent dark)
    execSync(`${IM} -size ${W}x${H} xc:transparent -background transparent -font "${FONT}" -pointsize ${fontSize} -fill "#00000099" -gravity Center -draw "text 0,0 '${escaped}'" "${tShadow}"`);

    // Top color layer — warm cream highlight (sampled from reference)
    execSync(`${IM} -size ${W}x${H} xc:transparent -background transparent -font "${FONT}" -pointsize ${fontSize} -fill "#f0e8da" -gravity Center -draw "text 0,0 '${escaped}'" "${tTop}"`);

    // Bottom color layer — deeper champagne/taupe (sampled from reference)
    execSync(`${IM} -size ${W}x${H} xc:transparent -background transparent -font "${FONT}" -pointsize ${fontSize} -fill "#a6957f" -gravity Center -draw "text 0,0 '${escaped}'" "${tBot}"`);

    // Vertical gradient mask (white-to-black = top to bottom)
    execSync(`${IM} -size ${W}x${H} gradient:"white-black" "${tVmask}"`);

    // Blend: bottom layer base, overlay top layer masked by gradient
    execSync(`${IM} "${tBot}" \\( "${tTop}" "${tVmask}" -alpha off -compose CopyOpacity -composite \\) -compose Over -composite "${tGold}"`);

    // Get text width to center it
    const meta = await sharp(tGold).metadata();
    const tw = meta.width || W;
    const left = Math.max(680, centerX - Math.floor(tw / 2));

    // Composite onto base image with sharp
    const buf = await sharp(BASE_IMG)
      .resize(OUT_W, OUT_H, { fit: 'fill' })
      .composite([
        { input: tShadow, left: left + 2, top: top + 5 },
        { input: tGold,   left,           top           },
      ])
      .jpeg({ quality: 93 })
      .toBuffer();

    cleanup();
    return buf;
  } catch (err) {
    cleanup();
    console.error('[sermon-thumb] ImageMagick failed:', err.message, '— falling back to sharp');
    return generateFallback(dateStr);
  }
}

/**
 * Fallback: sharp text input (may render sans-serif on servers without fontconfig).
 * Provides readable text even when ImageMagick is unavailable.
 */
async function generateFallback(dateStr) {
  const safe = String(dateStr).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const textBuf = await sharp({
    text: {
      text: `<span foreground="#e8dcb8">${safe}</span>`,
      font: 'Cinzel Bold',
      fontfile: FONT_PATH,
      width: DATE_TEXT_W,
      height: DATE_TEXT_H,
      rgba: true,
      align: 'centre',
    }
  }).png().toBuffer();
  const meta = await sharp(textBuf).metadata();
  const left = Math.max(680, DATE_CENTER_X - Math.floor((meta.width || DATE_TEXT_W) / 2));
  return sharp(BASE_IMG)
    .resize(OUT_W, OUT_H, { fit: 'fill' })
    .composite([{ input: textBuf, left, top: DATE_TOP_Y }])
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
