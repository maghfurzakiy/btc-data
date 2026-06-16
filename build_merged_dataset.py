#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BTC MERGED DATASET BUILDER (Pola A) — satu CSV siap-upload-ke-Claude.
- Tangkap SEGAR tiap dijalankan (Binance REST + HyperDash) -> baris 'live' di menit berjalan (zero-staleness).
- Arsip UPNL append-only + dedup + sorted (deterministik, ramah-merge git) -> history UPNL makin kaya.
- Grid per-jam: tiap jam memakai snapshot UPNL terbaru yang <= jam itu.
- Kolom 'source': hourly | live.
Dipakai sama oleh: cloud (GitHub Actions, hourly) & lokal (run_local.bat: pull->build->push).
Hanya pustaka standar Python.
"""
import json,csv,os,io,sys,time,zipfile,urllib.request
from datetime import datetime,timezone,timedelta

SYMBOL="BTCUSDT"; START="2026-01-24"     # HyperDash mulai ~24 Jan 2026; di sini baris cohort lengkap
BASE=os.path.dirname(os.path.abspath(__file__))
OUT=os.path.join(BASE,"btc_merged_hourly.csv"); ARCH=os.path.join(BASE,"upnl_history.csv")
CACHE=os.path.join(BASE,"cache"); os.makedirs(CACHE,exist_ok=True)
FAPI="https://fapi.binance.com"; VISION="https://data.binance.vision/data/futures/um/daily/metrics/%s/%s-metrics-%s.zip"
HD="https://api.hyperdash.com/graphql"
COHORTS=[("extremely_profitable","ep"),("very_profitable","vp"),("profitable","prof"),
         ("unprofitable","unprof"),("very_unprofitable","vu"),("rekt","rekt"),("apex","apex"),
         ("whale","whale"),("large","large"),("medium","medium"),("small","small")]
UA={"User-Agent":"Mozilla/5.0"}
def jget(url):
    with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=60) as r: return json.loads(r.read().decode())
def floorH(ts): return ts-(ts%3600)

def get_klines():
    start=int(datetime.strptime(START,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000); out={}
    while True:
        arr=jget(f"{FAPI}/fapi/v1/klines?symbol={SYMBOL}&interval=1h&startTime={start}&limit=1500")
        if not arr: break
        for k in arr: out[k[0]//1000]=dict(o=float(k[1]),h=float(k[2]),l=float(k[3]),c=float(k[4]),v=float(k[5]),tb=float(k[9]))
        if len(arr)<1500: break
        start=arr[-1][0]+1; time.sleep(0.3)
    cvd=0
    for ts in sorted(out): out[ts]["cvd"]=(cvd:=cvd+(2*out[ts]["tb"]-out[ts]["v"]))
    return out

def get_funding(hours):
    start=int(datetime.strptime(START,"%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()*1000); sett=[]
    while True:
        arr=jget(f"{FAPI}/fapi/v1/fundingRate?symbol={SYMBOL}&startTime={start}&limit=1000")
        if not arr: break
        for x in arr: sett.append((x["fundingTime"]//1000,float(x["fundingRate"])))
        if len(arr)<1000: break
        start=arr[-1]["fundingTime"]+1; time.sleep(0.3)
    sett.sort(); fr={}; j=0; last=0
    for ts in hours:
        while j<len(sett) and sett[j][0]<=ts: last=sett[j][1]; j+=1
        fr[ts]=last
    fr["_last"]=last; return fr

def get_metrics():
    m={}; d0=datetime.strptime(START,"%Y-%m-%d").date(); today=datetime.now(timezone.utc).date(); day=d0
    while day<=today:
        ds=day.strftime("%Y-%m-%d"); cf=os.path.join(CACHE,f"metrics-{ds}.csv"); txt=None
        if os.path.exists(cf): txt=open(cf,encoding="utf-8").read()
        else:
            try:
                with urllib.request.urlopen(urllib.request.Request(VISION%(SYMBOL,SYMBOL,ds),headers=UA),timeout=60) as r:
                    z=zipfile.ZipFile(io.BytesIO(r.read())); txt=z.read(z.namelist()[0]).decode()
                open(cf,"w",encoding="utf-8").write(txt)
            except Exception: txt=None
        if txt:
            for row in csv.DictReader(io.StringIO(txt)):
                try: ts=floorH(int(datetime.strptime(row["create_time"],"%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()))
                except Exception: continue
                m[ts]=dict(oi=row.get("sum_open_interest"),oiv=row.get("sum_open_interest_value"),
                    tt_acct=row.get("count_toptrader_long_short_ratio"),tt_pos=row.get("sum_toptrader_long_short_ratio"),
                    gl_acct=row.get("count_long_short_ratio"),taker=row.get("sum_taker_long_short_vol_ratio"))
        day+=timedelta(days=1)
    return m

def hd_post(op,vars,q):
    body=json.dumps({"operationName":op,"variables":vars,"query":q}).encode()
    req=urllib.request.Request(HD,data=body,headers={"Content-Type":"application/json","Origin":"https://hyperdash.com","User-Agent":"Mozilla/5.0"},method="POST")
    with urllib.request.urlopen(req,timeout=60) as r: return json.loads(r.read().decode())["data"]

def get_positioning():
    q="query HistoricalCohortPositioningV2($startTime: Float!){analytics{historicalCohortPositioningV2(startTime:$startTime){timestamp positioning __typename}__typename}}"
    pts=hd_post("HistoricalCohortPositioningV2",{"startTime":1672531200000},q)["analytics"]["historicalCohortPositioningV2"]; pos={}
    for p in pts:
        try: ts=floorH(int(datetime.strptime(p["timestamp"],"%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()))
        except Exception: continue
        row={}
        for cid,sh in COHORTS:
            c=p["positioning"].get(cid)
            if c:
                ln,s=c.get("longNotional",0),c.get("shortNotional",0); row[sh]=round(ln/(ln+s)*100,2) if (ln+s)>0 else ""
            else: row[sh]=""
        pos[ts]=row
    return pos

def capture_upnl_snapshot(price_now):
    q="query GetPnlCohort($id: String!){analytics{pnlCohort(id:$id){profitTraders lossTraders __typename}__typename}}"
    def pp(cid):
        try:
            d=hd_post("GetPnlCohort",{"id":cid},q)["analytics"]["pnlCohort"]; p,l=d.get("profitTraders") or 0,d.get("lossTraders") or 0
            return round(p/(p+l)*100,1) if (p+l)>0 else ""
        except Exception: return ""
    now=int(datetime.now(timezone.utc).timestamp())
    return [now,"live",round(price_now,1),pp("extremely_profitable"),pp("very_unprofitable")]

def update_archive(snap):
    rows={}
    if os.path.exists(ARCH):
        with open(ARCH,encoding="utf-8") as f:
            for r in csv.reader(f):
                if r and r[0]!="snapshot_ts":
                    key=int(r[0])//60*60; rows[key]=r       # dedup per-menit
    key=snap[0]//60*60; rows[key]=[snap[0],snap[1],snap[2],snap[3],snap[4]]
    ordered=[rows[k] for k in sorted(rows)]
    with open(ARCH,"w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["snapshot_ts","source","btc_price","ep_profPct","vu_profPct"]); w.writerows(ordered)
    # arsip terurut utk mapping per-jam: (ts, ep, vu)
    return [(int(r[0]),r[3],r[4]) for r in ordered]

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] build (Pola A) ...")
    kl=get_klines(); hours=sorted(kl); print(f"  klines: {len(kl)} jam")
    fr=get_funding(hours); print("  funding: ok")
    met=get_metrics(); print(f"  metrics: {len(met)} jam")
    pos=get_positioning(); print(f"  positioning: {len(pos)} jam")
    price_now=kl[hours[-1]]["c"]
    snap=capture_upnl_snapshot(price_now); arch=update_archive(snap); print(f"  UPNL arsip: {len(arch)} snapshot (+1 baru)")
    # mapping UPNL terbaru <= jam
    def upnl_asof(ts):
        ep=vu=""
        for s_ts,e,v in arch:
            if s_ts<=ts: ep,vu=e,v
            else: break
        return ep,vu
    cohort_cols=[s for _,s in COHORTS]
    cols=(["timestamp_utc","source","open","high","low","close","volume","taker_buy_vol","cvd","funding_rate",
           "oi","oi_value","toptrader_ls_acct","toptrader_ls_pos","global_ls_acct","taker_ls_ratio"]+cohort_cols+["ep_profPct","vu_profPct"])
    def metrics_asof(ts):
        if ts in met: return met[ts]
        prev=[t for t in met if t<=ts]; return met[max(prev)] if prev else {}
    n=0
    with open(OUT,"w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(cols)
        for ts in hours:
            k=kl[ts]; mm=met.get(ts,{}); pp=pos.get(ts,{}); ep,vu=upnl_asof(ts)
            iso=datetime.fromtimestamp(ts,timezone.utc).strftime("%Y-%m-%d %H:%M")
            row=[iso,"hourly",k["o"],k["h"],k["l"],k["c"],k["v"],k["tb"],round(k["cvd"],2),fr.get(ts,""),
                 mm.get("oi",""),mm.get("oiv",""),mm.get("tt_acct",""),mm.get("tt_pos",""),mm.get("gl_acct",""),mm.get("taker","")]
            row+=[pp.get(s,"") for s in cohort_cols]; row+=[ep,vu]; w.writerow(row); n+=1
        # baris LIVE (menit berjalan): klines jam berjalan (parsial) + segar semuanya
        lh=hours[-1]; k=kl[lh]; mm=metrics_asof(lh); latest_pos=pos.get(max(pos)) if pos else {}
        live_ts=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        row=[live_ts,"live",k["o"],k["h"],k["l"],k["c"],k["v"],k["tb"],round(k["cvd"],2),fr.get("_last",""),
             mm.get("oi",""),mm.get("oiv",""),mm.get("tt_acct",""),mm.get("tt_pos",""),mm.get("gl_acct",""),mm.get("taker","")]
        row+=[latest_pos.get(s,"") for s in cohort_cols]; row+=[snap[3],snap[4]]; w.writerow(row); n+=1
    print(f"  -> {OUT} ({n} baris, termasuk 1 baris live). Upload ke Claude.")

if __name__=="__main__": main()
