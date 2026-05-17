/**
 * Live Scripture Detection Service
 * 
 * Connects to a Mux live stream's HLS output, extracts audio,
 * sends to Deepgram for real-time transcription, detects scripture
 * references, and writes them to Firestore for app clients.
 * 
 * Firestore doc: live_scripture/{streamId}
 *   - current: { reference, book, chapter, verse, endVerse, verseText, detectedAt }
 *   - history: array of past references this session
 *   - active: boolean
 */

const { spawn } = require('child_process');
const WebSocket = require('ws');
const admin = require('firebase-admin');
const { detectScriptures } = require('./scripture-detector');

// KJV verse lookup via bible-api.com (free, no key needed)
async function fetchVerseText(reference) {
  try {
    const encoded = encodeURIComponent(reference);
    const res = await fetch(`https://bible-api.com/${encoded}?translation=kjv`);
    if (!res.ok) return null;
    const data = await res.json();
    return data.text?.trim() || null;
  } catch {
    return null;
  }
}

class LiveScriptureService {
  constructor({ deepgramApiKey, firestore }) {
    this.deepgramApiKey = deepgramApiKey;
    this.db = firestore;
    this.activeStreams = new Map(); // streamId -> { ffmpeg, ws, docRef }
    this.recentRefs = new Map();   // streamId -> Set of recent references (dedup window)
    this.streamStarts = new Map();  // streamId -> Mux stream created_at epoch (ms)
    this.audioOffsets = new Map();  // streamId -> ffmpeg audio start offset (seconds from stream start)
  }

  /**
   * Start monitoring a live stream for scripture references.
   * @param {string} streamId - Mux live stream ID
   * @param {string} playbackId - Mux playback ID for HLS access
   */
  async start(streamId, playbackId) {
    if (this.activeStreams.has(streamId)) {
      console.log(`[Scripture] Already monitoring stream ${streamId}`);
      return;
    }

    console.log(`[Scripture] Starting monitoring for stream ${streamId}`);

    const docRef = this.db.collection('live_scripture').doc(streamId);
    await docRef.set({
      active: true,
      current: null,
      history: [],
      startedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    this.recentRefs.set(streamId, new Set());
    this.audioOffsets.set(streamId, 0);  // Will be updated by ffmpeg progress

    // Get actual stream start time from Mux API
    let streamStartMs = Date.now();
    try {
      const MUX_TOKEN_ID = process.env.MUX_TOKEN_ID || '25cd1f0d-e6d4-445b-a106-e9ccc7a9f103';
      const MUX_SECRET = process.env.MUX_SECRET || '';
      const auth = Buffer.from(`${MUX_TOKEN_ID}:${MUX_SECRET}`).toString('base64');
      const res = await fetch(`https://api.mux.com/video/v1/live-streams/${streamId}`, {
        headers: { Authorization: `Basic ${auth}` },
      });
      const data = await res.json();
      // Mux returns created_at as unix epoch (seconds)
      if (data.data?.active_asset_start_time) {
        streamStartMs = data.data.active_asset_start_time * 1000;
        console.log(`[Scripture] Using Mux asset start time: ${new Date(streamStartMs).toISOString()}`);
      } else if (data.data?.created_at) {
        streamStartMs = data.data.created_at * 1000;
        console.log(`[Scripture] Using Mux stream created_at: ${new Date(streamStartMs).toISOString()}`);
      }
    } catch (e) {
      console.warn(`[Scripture] Could not fetch Mux stream start time: ${e.message}`);
    }
    this.streamStarts.set(streamId, streamStartMs);

    // Connect to Deepgram real-time WebSocket
    const dgUrl = 'wss://api.deepgram.com/v1/listen?' + new URLSearchParams({
      model: 'nova-3',
      language: 'en',
      smart_format: 'true',
      punctuate: 'true',
      interim_results: 'false',
      endpointing: '500',
      encoding: 's16le',
      sample_rate: '16000',
      channels: '1',
    }).toString();

    const ws = new WebSocket(dgUrl, {
      headers: { Authorization: `Token ${this.deepgramApiKey}` },
    });

    ws.on('open', () => {
      console.log(`[Scripture] Deepgram connected for stream ${streamId}`);
      this._startAudioPipe(streamId, playbackId, ws);
    });

    ws.on('message', async (data) => {
      try {
        const msg = JSON.parse(data);
        if (msg.type !== 'Results' || !msg.channel?.alternatives?.[0]) return;
        
        const alt = msg.channel.alternatives[0];
        const transcript = alt.transcript;
        if (!transcript || transcript.trim().length < 3) return;

        // Deepgram returns `start` = seconds from beginning of the audio stream
        // This maps directly to the HLS/VOD timeline position
        const dgStartSec = msg.start || 0;

        console.log(`[Scripture] [${Math.round(dgStartSec)}s] Transcript: "${transcript}"`);

        const refs = detectScriptures(transcript);
        if (refs.length === 0) return;

        // Dedup: skip references we've shown in the last 2 minutes
        const recent = this.recentRefs.get(streamId) || new Set();
        const newRefs = refs.filter(r => !recent.has(r.reference));
        if (newRefs.length === 0) return;

        for (const ref of newRefs) {
          recent.add(ref.reference);
          // Clear from dedup set after 2 minutes
          setTimeout(() => recent.delete(ref.reference), 120000);

          // Fetch KJV verse text
          const verseText = await fetchVerseText(ref.reference);

          // Use Deepgram's audio position (seconds from stream start)
          // This aligns with the VOD asset timeline since both start from
          // the beginning of the Mux recording
          const offsetSeconds = Math.round(dgStartSec);

          const entry = {
            ...ref,
            verseText: verseText || null,
            detectedAt: new Date().toISOString(),
            offsetSeconds,
          };

          console.log(`[Scripture] Detected: ${ref.reference} @ ${offsetSeconds}s${verseText ? ' ✓ text' : ''}`);

          // Update Firestore
          await docRef.update({
            current: entry,
            history: admin.firestore.FieldValue.arrayUnion(entry),
          });
        }
      } catch (e) {
        console.warn('[Scripture] Message parse error:', e.message);
      }
    });

    ws.on('close', () => {
      console.log(`[Scripture] Deepgram disconnected for stream ${streamId}`);
      this.stop(streamId);
    });

    ws.on('error', (err) => {
      console.error(`[Scripture] Deepgram error for stream ${streamId}:`, err.message);
    });

    this.activeStreams.set(streamId, { ws, docRef, ffmpeg: null });
  }

  _startAudioPipe(streamId, playbackId, ws) {
    const hlsUrl = `https://stream.mux.com/${playbackId}.m3u8`;

    // Use ffmpeg to extract audio from HLS and pipe as raw PCM
    const ffmpeg = spawn('ffmpeg', [
      '-i', hlsUrl,
      '-vn',                    // no video
      '-acodec', 'pcm_s16le',  // raw PCM 16-bit
      '-ar', '16000',           // 16kHz sample rate
      '-ac', '1',               // mono
      '-f', 's16le',            // raw format
      'pipe:1',                 // output to stdout
    ], { stdio: ['ignore', 'pipe', 'pipe'] });

    ffmpeg.stdout.on('data', (chunk) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(chunk);
      }
    });

    ffmpeg.stderr.on('data', (data) => {
      // ffmpeg logs to stderr — only log errors
      const msg = data.toString();
      if (msg.includes('Error') || msg.includes('error')) {
        console.error(`[Scripture] ffmpeg: ${msg.trim()}`);
      }
    });

    ffmpeg.on('close', (code) => {
      console.log(`[Scripture] ffmpeg exited with code ${code} for stream ${streamId}`);
      if (this.activeStreams.has(streamId)) {
        // Stream might have ended — clean up
        this.stop(streamId);
      }
    });

    const entry = this.activeStreams.get(streamId);
    if (entry) entry.ffmpeg = ffmpeg;
  }

  async stop(streamId) {
    const entry = this.activeStreams.get(streamId);
    if (!entry) return;

    console.log(`[Scripture] Stopping monitoring for stream ${streamId}`);

    if (entry.ffmpeg) {
      try { entry.ffmpeg.kill('SIGTERM'); } catch {}
    }
    if (entry.ws) {
      try { entry.ws.close(); } catch {}
    }

    // Mark inactive in Firestore
    try {
      await entry.docRef.update({
        active: false,
        current: null,
        endedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
    } catch {}

    this.activeStreams.delete(streamId);
    this.recentRefs.delete(streamId);
    this.streamStarts.delete(streamId);
    this.audioOffsets.delete(streamId);
  }

  stopAll() {
    for (const streamId of this.activeStreams.keys()) {
      this.stop(streamId);
    }
  }

  isActive(streamId) {
    return this.activeStreams.has(streamId);
  }

  getActiveStreams() {
    return Array.from(this.activeStreams.keys());
  }
}

module.exports = LiveScriptureService;
