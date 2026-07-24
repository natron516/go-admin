#!/usr/bin/env python3
import json,urllib.request,base64,re,sys,time
BASE="https://go-admin-production-6be4.up.railway.app"
AUTH="Basic "+base64.b64encode(b"admin:gomedia").decode()
def req(path,method="GET",body=None,timeout=900):
    r=urllib.request.Request(BASE+path,method=method,headers={"Authorization":AUTH,"Content-Type":"application/json"},
        data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(r,timeout=timeout) as resp:
            return resp.status,json.loads(resp.read().decode() or "{}")
    except Exception as e:
        return getattr(e,"code",0),{"error":str(e)}
def items():
    _,d=req("/api/audio")
    out=[]
    for a in d["audio"]:
        if a.get("category")!="archive_sermon": continue
        m=re.search(r"stream\.mux\.com/([^./]+)\.m3u8",a.get("audioUrl") or "")
        out.append((a["id"][:8],a.get("title"),m.group(1) if m else None,a.get("duration") or 0))
    return out
def status(pid,dur):
    _,d=req(f"/api/audio-transcript/{pid}/cues")
    st=d.get("status")
    cues=d.get("cues") or []
    last=cues[-1]["end"] if cues else 0
    gap=max((cues[i+1]["start"]-cues[i]["end"] for i in range(len(cues)-1)),default=0)
    return st,len(cues),last,gap
if __name__=="__main__":
    for iid,title,pid,dur in items():
        if not pid:
            print(f"{iid} {title!r}: NO AUDIO URL"); continue
        st,n,last,gap=status(pid,dur)
        cov="?" if not dur else f"{dur-last:.0f}s short" 
        print(f"{iid} {title!r} pid={pid[:12]}… status={st} cues={n} lastEnd={last:.0f} dur={dur:.0f} ({cov}) maxgap={gap:.1f}")
