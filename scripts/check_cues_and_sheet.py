#!/usr/bin/env python3
"""
Check /cues for all archive_sermon assets and determine which sheet rows to mark as Transcribed=TRUE.
Outputs a JSON file with date->ready mapping.
"""
import json
import re
import urllib.request
import urllib.parse
import base64
import sys

PORTAL = "https://go-admin-production-6be4.up.railway.app"
AUTH = base64.b64encode(b"admin:gomedia").decode()

def api_get(path):
    req = urllib.request.Request(f"{PORTAL}{path}", headers={"Authorization": f"Basic {AUTH}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

# Get all assets
print("Fetching assets...", file=sys.stderr)
data = api_get("/api/assets")
assets = data.get("data", [])
archive = [a for a in assets if "archive_sermon" in str(a.get("passthrough", ""))]
print(f"Found {len(archive)} archive_sermon assets", file=sys.stderr)

# Build date->pid map
date_pid_map = {}  # YYYY-MM-DD -> [pid, ...]
pid_date_map = {}  # pid -> YYYY-MM-DD

for a in archive:
    pid = (a.get("playback_ids") or [{}])[0].get("id", "")
    if not pid:
        continue
    pt_raw = a.get("passthrough", "")
    pt = {}
    try:
        pt = json.loads(pt_raw)
    except Exception:
        try:
            pt = dict(urllib.parse.parse_qsl(pt_raw))
        except Exception:
            pass
    title = pt.get("title", "")
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})", title)
    if m:
        date_str = f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        date_pid_map.setdefault(date_str, []).append(pid)
        pid_date_map[pid] = date_str

print(f"Assets with extractable dates: {len(pid_date_map)}", file=sys.stderr)

# Check /cues for each PID
ready_dates = set()
not_ready_dates = {}

for pid, date in sorted(pid_date_map.items(), key=lambda x: x[1]):
    try:
        cues_data = api_get(f"/api/audio-transcript/{pid}/cues")
        status = cues_data.get("status", "unknown")
        cue_count = len(cues_data.get("cues", []))
        print(f"  {date} | {pid[:20]}... | status={status} cues={cue_count}", file=sys.stderr)
        if status == "ready":
            ready_dates.add(date)
        else:
            not_ready_dates[date] = status
    except Exception as e:
        print(f"  {date} | {pid[:20]}... | ERROR: {e}", file=sys.stderr)
        not_ready_dates[date] = f"error: {e}"

result = {
    "ready_dates": sorted(ready_dates),
    "not_ready": not_ready_dates,
    "total_assets": len(archive),
    "assets_with_dates": len(pid_date_map),
}

print(json.dumps(result, indent=2))
