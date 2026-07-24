#!/bin/bash
# Upgrade 28 archive_sermon assets from Mux-generated captions to Deepgram
# Pipeline: POST /api/transcript-redo (Deepgram words) -> POST /api/sermons/use-deepgram-captions
# Run serially with polite pacing

PORTAL="https://go-admin-production-6be4.up.railway.app"
AUTH="admin:gomedia"
LOG_FILE="/Users/natewilliams/.openclaw/agents/gamedev/workspace/go-admin/scripts/upgrade_archive_sermons_$(date +%Y%m%d_%H%M%S).log"

PIDS=(
  "00qDsD7y5llxgYo00daQjYp5wxy02ulkNbuCVB8RHYjTq8"
  "rE53DCJSac126GX5XyA02pM4021cc8fovfTPiMlTQk7fY"
  "usJzp4A00AfAiKpxwDsn1Cv4BZIxpcXtaLArjY01UzJPw"
  "qWEuoc00bfwkVdyt00oA1XcmtvsJdf1wiye9pe2sHwuEU"
  "XD38Ch5IEJQdlNs37eaj02OJMHEDbNagmeBW02e3Z5TKQ"
  "FjcMmOxi7cFKVBWUBcdPfVLHt1Iq02cPT008SHXD4shqM"
  "ofxE8aKq6gBxbFBoWutEFDG9006SqFqqQUDY2vTVn7LU"
  "b00yMgC6ULEubjZizECf700NdC6OiIVCWCrz1NgzNOvu00"
  "00DuTIzdn6j7hOfRyEEPvzB8OvVaYSzdyGnMD5Gqcxek"
  "gf6AGfdsUop4H4cC2yrJuWKVePpC1BVvpYxiBbfSqtY"
  "wjHBMEcLhFgIi71BwlHpx4RrBQkpx3V7KrqJ00vly1N8"
  "kwkI00qCHkTJJAW375eF7q2NtU32BAHwwAvnpaYgwpxo"
  "1kiY4yaGakrOX1ozgeK0202a5xei8t6hp2ibZfLoB01a6Y"
  "3JFaxUe9qmefE551Nk00sRQLv6JPTTZoFz01ycfA01mORA"
  "zmicnm9LN8152ytuAy6HWFD00Q3HyzTsU01xB9dDA4Qn8"
  "vD6KlK01qOEIvO22J5EtulCV8vJIuWZG4ofZt86MLFAs"
  "015joKPoeTGdEt7WiH41gaEy3C6hlSeHgualNoZKlP9s"
  "vbzgX2COV6v02NwYLinFwa5I0101jdqsl02Fc02zLeyJ01XSs"
  "Pr4dMgoyX9mlZgsggpJ58XRvrUG3Im9dC022UQMHu022E"
  "O6I01WLKkQ8XwRzkCM8WYbP027WRgsXQV7MJBWMHV3KVg"
  "Ow01DdswOzir15dmzYcOfXq745ljAootVoIaOAD9EcTI"
  "vF7nwy1ImkXWMqsmbwt801cHmwVZgojsKBR01Rw00diCA8"
  "jkycl00Q9zJ5U9TA2sLhfADvF8LhAZVQWtGP8evz5Edw"
  "iqe5id6YKBpJFoLdzEotL69gToe1019OeX1a7jqLPzww"
  "ufw3ZFi9MIAdVnZt5NMvZFuragCxD8c86W7RpYuUYKc"
  "W1MLc5nBlB9Jb3HyIuF7SJZk21SgAqypvsI3dqxb3rc"
  "cQJOAH5Lk1SSN5JXQgw2GeVfBsVvRYwxmQgGTQrd02300"
  "oyfZSATfpbYAE21RrViPdbwxLSXi77fmzAg02fBDdw7Q"
)

echo "=== ARCHIVE SERMON DEEPGRAM UPGRADE ===" | tee "$LOG_FILE"
echo "Started: $(date)" | tee -a "$LOG_FILE"
echo "Total PIDs: ${#PIDS[@]}" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

success=0
failed=0
skipped=0

for i in "${!PIDS[@]}"; do
  pid="${PIDS[$i]}"
  n=$((i+1))
  echo "[$n/${#PIDS[@]}] Processing $pid ..." | tee -a "$LOG_FILE"
  
  # Step 1: transcript-redo (Deepgram words, force=true)
  echo "  Step 1: transcript-redo ..." | tee -a "$LOG_FILE"
  redo_resp=$(curl -s -X POST -u "$AUTH" "$PORTAL/api/transcript-redo" \
    -H "Content-Type: application/json" \
    -d "{\"playbackId\":\"$pid\",\"force\":true}" \
    --max-time 300)
  
  redo_ok=$(echo "$redo_resp" | python3 -c "import json,sys; d=json.load(sys.stdin); print('yes' if d.get('ok') else 'no')" 2>/dev/null)
  redo_words=$(echo "$redo_resp" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('words',d.get('count','?')))" 2>/dev/null)
  
  echo "  transcript-redo result: ok=$redo_ok words=$redo_words" | tee -a "$LOG_FILE"
  echo "  Full redo response: $redo_resp" >> "$LOG_FILE"
  
  if [ "$redo_ok" != "yes" ]; then
    echo "  ❌ transcript-redo FAILED for $pid" | tee -a "$LOG_FILE"
    echo "  Response: $redo_resp" | tee -a "$LOG_FILE"
    ((failed++))
    sleep 3
    continue
  fi
  
  # Step 2: swap to Deepgram captions
  echo "  Step 2: use-deepgram-captions ..." | tee -a "$LOG_FILE"
  swap_resp=$(curl -s -X POST -u "$AUTH" "$PORTAL/api/sermons/use-deepgram-captions" \
    -H "Content-Type: application/json" \
    -d "{\"playbackId\":\"$pid\"}" \
    --max-time 60)
  
  swap_ok=$(echo "$swap_resp" | python3 -c "import json,sys; d=json.load(sys.stdin); print('yes' if d.get('ok') else 'no')" 2>/dev/null)
  swap_cues=$(echo "$swap_resp" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('cues','?'))" 2>/dev/null)
  
  echo "  swap result: ok=$swap_ok cues=$swap_cues" | tee -a "$LOG_FILE"
  echo "  Full swap response: $swap_resp" >> "$LOG_FILE"
  
  if [ "$swap_ok" = "yes" ]; then
    echo "  ✅ SUCCESS: words=$redo_words cues=$swap_cues" | tee -a "$LOG_FILE"
    ((success++))
  else
    echo "  ❌ swap FAILED for $pid" | tee -a "$LOG_FILE"
    echo "  Response: $swap_resp" | tee -a "$LOG_FILE"
    ((failed++))
  fi
  
  echo "" | tee -a "$LOG_FILE"
  
  # Polite pacing - Deepgram transcribes quickly but let's be gentle
  if [ $n -lt ${#PIDS[@]} ]; then
    sleep 5
  fi
done

echo "=== FINAL RESULTS ===" | tee -a "$LOG_FILE"
echo "Success: $success" | tee -a "$LOG_FILE"
echo "Failed: $failed" | tee -a "$LOG_FILE"
echo "Skipped: $skipped" | tee -a "$LOG_FILE"
echo "Finished: $(date)" | tee -a "$LOG_FILE"
echo "Log: $LOG_FILE"
