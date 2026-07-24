#!/bin/bash
BASE="https://go-admin-production-6be4.up.railway.app"; AUTH="admin:gomedia"
for pid in lBARSztUSgY7V3Ea01CJJ1DyxhttpQCqjVe01bOSvRMD4 y01Zc3tQUI78dCwen6FPR46XNESfbcGVvSMfIpYNsEVc l5g4NntypAaOzxIKuQoEg43cFw5RGACKwczn55V00Oo00 hAFshvX8lZmNDytR01Wpl394YwMGegeVu8vloZ8nTWmI YKm5dC3IHJf6402JYCtHQ012JROu8fgZ8G6NO02cPPMBgQ SKJdo2yGMO5X102Pq2vvNu9axxcQ58NeYpIGhp8t3pGY; do
  echo "=== $(date +%H:%M:%S) $pid transcript-redo"
  curl -s -u $AUTH -X POST -H 'Content-Type: application/json' -d "{\"playbackId\":\"$pid\",\"force\":true}" "$BASE/api/transcript-redo" --max-time 1200 | head -c 200; echo
  curl -s -u $AUTH -X POST -H 'Content-Type: application/json' -d "{\"playbackId\":\"$pid\"}" "$BASE/api/sermons/use-deepgram-captions" --max-time 600 | head -c 200; echo
done
echo "KICKS DONE"
