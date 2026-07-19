#!/bin/bash
# Olga: wait for Mux static renditions, then run Deepgram transcript pipeline.
BASE="https://go-admin-production-6be4.up.railway.app"
AUTH="admin:gomedia"
PAIRS=(
"ocD1fZ2m4R3SBYb7CM02B6COfe00UfrKJoxkztpYB02akU 2kw2ishARiEuo7z8dXtWjKQAGGr8qWxum02nG00pZpuNs"
"E7JMRr2UKhKjYphWysatp2QL9I02VKM29PDtWHKqJkOA YWOmo6bJX001SvsB1AY63bThPbA5XVhdVaZFIcgdyJyQ"
"r7HndDzfXOhNVz3nHhosXB9P2bC92yA15hZLbR0102HQo 701O006qAwxgqPMoIOfNtKFFKIMZogsEc3IWjHA3x76X00"
)
DEADLINE=$(( $(date +%s) + 2700 ))  # 45 min
DONE_LIST=""
while [ $(date +%s) -lt $DEADLINE ]; do
  alldone=1
  for pair in "${PAIRS[@]}"; do
    set -- $pair; aid=$1; pid=$2
    case " $DONE_LIST " in *" $aid "*) continue;; esac
    ready=0
    for f in audio.m4a low.mp4 medium.mp4; do
      code=$(curl -s -o /dev/null -w "%{http_code}" -I "https://stream.mux.com/$pid/$f")
      [ "$code" = "200" ] && ready=1 && break
    done
    if [ $ready = 1 ]; then
      echo "=== $(date +%H:%M:%S) $aid rendition ready ($f) ==="
      # skip if auto path already produced cues
      st=$(curl -s -u $AUTH "$BASE/api/audio-transcript/$pid/cues" | python3 -c "import json,sys;print(json.load(sys.stdin).get('status'))")
      if [ "$st" = "ready" ] || [ "$st" = "preparing" ]; then
        echo "$aid: auto path already ran (status=$st) — skipping manual kick"
      else
        echo "$aid: running transcript-redo..."
        curl -s -u $AUTH -X POST -H 'Content-Type: application/json' -d "{\"playbackId\":\"$pid\",\"force\":true}" "$BASE/api/transcript-redo" --max-time 900 | head -c 200; echo
        curl -s -u $AUTH -X POST -H 'Content-Type: application/json' -d "{\"playbackId\":\"$pid\"}" "$BASE/api/sermons/use-deepgram-captions" --max-time 300 | head -c 200; echo
      fi
      DONE_LIST="$DONE_LIST $aid"
    else
      alldone=0
    fi
  done
  [ $alldone = 1 ] && echo "ALL DONE" && exit 0
  sleep 180
done
echo "TIMEOUT — some assets stalled"
for pair in "${PAIRS[@]}"; do set -- $pair; case " $DONE_LIST " in *" $1 "*) ;; *) echo "STALLED: $1";; esac; done
