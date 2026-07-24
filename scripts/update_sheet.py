#!/usr/bin/env python3
"""
Update the Transcribed column (D) in the GO Sermon Archive Upload Tracker sheet
for all rows whose date matches a ready archive_sermon asset.
"""
import json
import subprocess
import sys

SHEET_ID = "1tTLoPc2MtOIXuBccqol8FSSwqruCGvWIwM3RqVneyaM"

# Ready dates from cues check
READY_DATES = set([
    "2017-07-16", "2017-08-06", "2017-08-13", "2017-08-20", "2017-08-27",
    "2017-09-03", "2017-09-10", "2017-09-17", "2017-09-24", "2017-10-08",
    "2017-10-15", "2017-10-22", "2017-10-29", "2017-11-05", "2017-11-12",
    "2017-11-19", "2017-11-26", "2017-12-03", "2017-12-10", "2017-12-17",
    "2017-12-24", "2017-12-31",
    "2018-01-07", "2018-01-14", "2018-01-21", "2018-01-28",
    "2018-02-04", "2018-02-18", "2018-02-25",
    "2018-03-04", "2018-03-11", "2018-03-25",
    "2018-04-08", "2018-04-15", "2018-04-22", "2018-04-29",
    "2018-05-06", "2018-05-13", "2018-05-20", "2018-05-27",
    "2018-06-03", "2018-06-10", "2018-06-17", "2018-06-24",
    "2018-07-01", "2018-07-08", "2018-07-15",
    "2018-08-05", "2018-08-12", "2018-08-26",
    "2018-09-02", "2018-09-23", "2018-09-30",
    "2018-10-07", "2018-10-14", "2018-10-21", "2018-10-28",
    "2018-11-04", "2018-11-11", "2018-11-25",
    "2018-12-09", "2018-12-16", "2018-12-30",
    "2019-03-10", "2019-08-04",
])

def gog_sheets_get(range_str):
    result = subprocess.run(
        ["gog", "sheets", "get", SHEET_ID, range_str, "--json"],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        raise RuntimeError(f"gog sheets get failed: {result.stderr}")
    return json.loads(result.stdout)

def gog_sheets_update(range_str, values):
    values_json = json.dumps(values)
    result = subprocess.run(
        ["gog", "sheets", "update", SHEET_ID, range_str,
         "--values-json", values_json, "--input", "USER_ENTERED"],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        raise RuntimeError(f"gog sheets update failed: {result.stderr}")
    return result.stdout

# Read all sheet data
print("Reading sheet data...", file=sys.stderr)
data = gog_sheets_get("Sheet1!A1:E200")
rows = data.get("values", [])
print(f"Got {len(rows)} rows", file=sys.stderr)

# Find rows to update
updates = []  # (row_num_1indexed, current_transcribed, new_transcribed)
already_true = 0
will_update = 0
no_match = 0

for i, row in enumerate(rows[1:], start=2):  # skip header, 1-indexed
    if not row or not row[0].strip():
        continue
    date_val = row[0].strip()
    uploaded = row[2].strip() if len(row) > 2 else ""
    current_transcribed = row[3].strip() if len(row) > 3 else ""

    if date_val in READY_DATES:
        if current_transcribed == "TRUE":
            already_true += 1
        else:
            updates.append((i, date_val, current_transcribed))
            will_update += 1
    else:
        no_match += 1

print(f"Already TRUE: {already_true}", file=sys.stderr)
print(f"Will update to TRUE: {will_update}", file=sys.stderr)
print(f"Rows without matching asset (leave as-is): {no_match}", file=sys.stderr)

# Apply updates - update each row individually
updated = 0
failed = 0
for row_num, date_val, old_val in updates:
    cell = f"Sheet1!D{row_num}"
    try:
        gog_sheets_update(cell, [["TRUE"]])
        print(f"  ✅ Row {row_num} ({date_val}): {old_val} -> TRUE", file=sys.stderr)
        updated += 1
    except Exception as e:
        print(f"  ❌ Row {row_num} ({date_val}): FAILED: {e}", file=sys.stderr)
        failed += 1

print(f"\nSummary: updated={updated} already_true={already_true} failed={failed}", file=sys.stderr)
print(json.dumps({"updated": updated, "already_true": already_true, "failed": failed, "will_update": will_update}))
