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

=== PATCH 1.43 (short-notional / ECC leg-2) ===
Endpoint positioning (historical*) return shortNotional FULL-HISTORY tiap run, tapi versi lama
cuma pakai 's' buat ratio long% lalu MEMBUANGnya. Patch ini MENYIMPAN short-notional $ mentah
sebagai kolom baru *_sn (agregat) dan *_btc_sn (BTC-only), di-APPEND di akhir skema (tidak
mengubah urutan kolom lama -> aman utk journal & CSV historis).
- SN_COHORTS = cohort mana yang di-emit short-notional-nya. Default ["ep"] (yang dibutuhkan ECC).
  Set ke [s for _,s in COHORTS] kalau mau SEMUA cohort.
- Karena endpoint-nya historical, kolom *_sn LANGSUNG TERISI untuk seluruh seri (backfillable),
  beda dgn ep_profPct (snapshot-only) yang tetap harus akumulasi 24/7.
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
# >>> PATCH: cohort yang di-emit short-notional ($). Default cuma EP (cukup utk ECC leg-2).
#     Ganti ke [s for _,s in COHORTS] kalau mau SEMUA cohort.
SN_COHORTS=["ep"]
UA={"User-Agent":"Mozilla/5.0"}
import ssl, urllib.error

# --- TLS context cascade: fix WinError 10054. CloudFront/AWS minta TLS1.2 renegotiation yg ditolak
#     OpenSSL (urllib & requests sama-sama OpenSSL). TLS1.3 MENGHAPUS renegotiation dari protokol,
#     jadi handshake mulus. Coba TLS1.3 dulu -> fallback TLS1.2-legacy -> default. Pemenang di-cache.
#     STDLIB MURNI (requests tidak lagi diperlukan).
def _ctx_tls13():
    c = ssl.create_default_context(); c.minimum_version = ssl.TLSVersion.TLSv1_3; return c

def _ctx_tls12_legacy():
    c = ssl.create_default_context()
    c.minimum_version = ssl.TLSVersion.TLSv1_2; c.maximum_version = ssl.TLSVersion.TLSv1_2
    c.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0)
    c.options &= ~getattr(ssl, "OP_NO_RENEGOTIATION", 0)
    try: c.set_ciphers("DEFAULT@SECLEVEL=1")
    except ssl.SSLError: pass
    return c

_CTX_BUILDERS = [("TLS1.3", _ctx_tls13), ("TLS1.2-legacy", _ctx_tls12_legacy),
                 ("default", ssl.create_default_context)]
_CTX_WINNER = None

def _fetch(req, tries=5, backoff=2.0, timeout=60):
    """retry+backoff + TLS-context cascade (fix WinError 10054 reneg OpenSSL).
       req = urllib.request.Request (kompat call-site lama)."""
    global _CTX_WINNER
    order = list(range(len(_CTX_BUILDERS)))
    if _CTX_WINNER is not None:
        order = [_CTX_WINNER] + [i for i in order if i != _CTX_WINNER]
    last = None
    for attempt in range(tries):
        for i in order:
            name, build = _CTX_BUILDERS[i]
            try:
                with urllib.request.urlopen(req, timeout=timeout, context=build()) as r:
                    if _CTX_WINNER != i:
                        _CTX_WINNER = i; print(f"    [tls] konteks dipakai: {name}")
                    return r.read()
            except urllib.error.HTTPError:
                _CTX_WINNER = i; raise   # HTTP 4xx/5xx = TLS sukses (mis. Vision 404) -> jangan coba ctx lain
            except Exception as e:
                last = e
        print(f"    retry {attempt+1}/{tries} (semua ctx gagal: {type(last).__name__}) ...")
        time.sleep(backoff*(attempt+1))
    raise last
def jget(url): return json.loads(_fetch(urllib.request.Request(url,headers=UA)).decode())
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
                raw=_fetch(urllib.request.Request(VISION%(SYMBOL,SYMBOL,ds),headers=UA),tries=2,backoff=1.0)
                z=zipfile.ZipFile(io.BytesIO(raw)); txt=z.read(z.namelist()[0]).decode()
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
    return json.loads(_fetch(req).decode())["data"]

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
                ln,s=c.get("longNotional",0),c.get("shortNotional",0)
                row[sh]=round(ln/(ln+s)*100,2) if (ln+s)>0 else ""
                if sh in SN_COHORTS:
                    row[sh+"_sn"]=round(float(s or 0),0)    # >>> PATCH leg-2 short-notional (agregat)
                    row[sh+"_ln"]=round(float(ln or 0),0)   # >>> PATCH long-notional (agregat)
            else:
                row[sh]=""
                if sh in SN_COHORTS:
                    row[sh+"_sn"]=""; row[sh+"_ln"]=""
        pos[ts]=row
    return pos

def get_positioning_btc():
    # BTC-only cohort positioning (historicalCohortPositioningByMarket) -> long% per cohort, kolom *_btc
    q=("query HistByMarket($coin: String!, $startTime: Float!){analytics{"
       "historicalCohortPositioningByMarket(coin:$coin,startTime:$startTime){"
       "timestamp pnlCohorts{cohortId longNotional shortNotional} "
       "sizeCohorts{cohortId longNotional shortNotional}}}}")
    pts=hd_post("HistByMarket",{"coin":"BTC","startTime":1672531200000},q)["analytics"]["historicalCohortPositioningByMarket"]; pos={}
    for p in pts:
        try: ts=floorH(int(datetime.strptime(p["timestamp"],"%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()))
        except Exception: continue
        flat={}
        for c in (p.get("pnlCohorts") or [])+(p.get("sizeCohorts") or []): flat[c["cohortId"]]=c
        row={}
        for cid,sh in COHORTS:
            c=flat.get(cid)
            if c:
                ln,s=float(c.get("longNotional",0) or 0),float(c.get("shortNotional",0) or 0)
                row[sh]=round(ln/(ln+s)*100,2) if (ln+s)>0 else ""
                if sh in SN_COHORTS:
                    row[sh+"_sn"]=round(s,0)               # >>> PATCH leg-2 short-notional (BTC-only)
                    row[sh+"_ln"]=round(ln,0)              # >>> PATCH long-notional (BTC-only)
            else:
                row[sh]=""
                if sh in SN_COHORTS:
                    row[sh+"_sn"]=""; row[sh+"_ln"]=""
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
    _atmp=ARCH+".tmp"
    with open(_atmp,"w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["snapshot_ts","source","btc_price","ep_profPct","vu_profPct"]); w.writerows(ordered)
    os.replace(_atmp,ARCH)
    # arsip terurut utk mapping per-jam: (ts, ep, vu)
    return [(int(r[0]),r[3],r[4]) for r in ordered]

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] build (Pola A) ...")
    kl=get_klines(); hours=sorted(kl); print(f"  klines: {len(kl)} jam")
    fr=get_funding(hours); print("  funding: ok")
    met=get_metrics(); print(f"  metrics: {len(met)} jam")
    pos=get_positioning(); print(f"  positioning (agregat): {len(pos)} jam")
    try: pos_btc=get_positioning_btc(); print(f"  positioning (BTC-only): {len(pos_btc)} jam")
    except Exception as e: pos_btc={}; print(f"  positioning (BTC-only): GAGAL ({e}) -> kolom *_btc kosong")
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
    btc_cohort_cols=[s+"_btc" for _,s in COHORTS]
    sn_cols=[s+"_sn" for s in SN_COHORTS]            # >>> PATCH: short-notional agregat (ep_sn, ...)
    sn_btc_cols=[s+"_btc_sn" for s in SN_COHORTS]    # >>> PATCH: short-notional BTC-only (ep_btc_sn, ...)
    ln_cols=[s+"_ln" for s in SN_COHORTS]            # >>> PATCH: long-notional agregat (ep_ln, ...)
    ln_btc_cols=[s+"_btc_ln" for s in SN_COHORTS]    # >>> PATCH: long-notional BTC-only (ep_btc_ln, ...)
    cols=(["timestamp_utc","source","open","high","low","close","volume","taker_buy_vol","cvd","funding_rate",
           "oi","oi_value","toptrader_ls_acct","toptrader_ls_pos","global_ls_acct","taker_ls_ratio"]
          +cohort_cols+["ep_profPct","vu_profPct"]+btc_cohort_cols
          +sn_cols+sn_btc_cols+ln_cols+ln_btc_cols)   # >>> PATCH: append di akhir (skema lama utuh)
    def metrics_asof(ts):
        if ts in met: return met[ts]
        prev=[t for t in met if t<=ts]; return met[max(prev)] if prev else {}
    n=0
    _tmp=OUT+".tmp"
    with open(_tmp,"w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(cols)
        for ts in hours:
            k=kl[ts]; mm=met.get(ts,{}); pp=pos.get(ts,{}); ep,vu=upnl_asof(ts)
            iso=datetime.fromtimestamp(ts,timezone.utc).strftime("%Y-%m-%d %H:%M")
            row=[iso,"hourly",k["o"],k["h"],k["l"],k["c"],k["v"],k["tb"],round(k["cvd"],2),fr.get(ts,""),
                 mm.get("oi",""),mm.get("oiv",""),mm.get("tt_acct",""),mm.get("tt_pos",""),mm.get("gl_acct",""),mm.get("taker","")]
            ppb=pos_btc.get(ts,{})
            row+=[pp.get(s,"") for s in cohort_cols]; row+=[ep,vu]; row+=[ppb.get(s,"") for s in cohort_cols]
            row+=[pp.get(s+"_sn","") for s in SN_COHORTS]          # >>> PATCH: ep_sn ...
            row+=[ppb.get(s+"_sn","") for s in SN_COHORTS]         # >>> PATCH: ep_btc_sn ...
            row+=[pp.get(s+"_ln","") for s in SN_COHORTS]          # >>> PATCH: ep_ln ...
            row+=[ppb.get(s+"_ln","") for s in SN_COHORTS]         # >>> PATCH: ep_btc_ln ...
            w.writerow(row); n+=1
        # baris LIVE (menit berjalan): klines jam berjalan (parsial) + segar semuanya
        lh=hours[-1]; k=kl[lh]; mm=metrics_asof(lh)
        latest_pos=pos.get(max(pos)) if pos else {}; latest_pos_btc=pos_btc.get(max(pos_btc)) if pos_btc else {}
        live_ts=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
        row=[live_ts,"live",k["o"],k["h"],k["l"],k["c"],k["v"],k["tb"],round(k["cvd"],2),fr.get("_last",""),
             mm.get("oi",""),mm.get("oiv",""),mm.get("tt_acct",""),mm.get("tt_pos",""),mm.get("gl_acct",""),mm.get("taker","")]
        row+=[latest_pos.get(s,"") for s in cohort_cols]; row+=[snap[3],snap[4]]; row+=[latest_pos_btc.get(s,"") for s in cohort_cols]
        row+=[latest_pos.get(s+"_sn","") for s in SN_COHORTS]      # >>> PATCH live: ep_sn
        row+=[latest_pos_btc.get(s+"_sn","") for s in SN_COHORTS]  # >>> PATCH live: ep_btc_sn
        row+=[latest_pos.get(s+"_ln","") for s in SN_COHORTS]      # >>> PATCH live: ep_ln
        row+=[latest_pos_btc.get(s+"_ln","") for s in SN_COHORTS]  # >>> PATCH live: ep_btc_ln
        w.writerow(row); n+=1
    os.replace(_tmp,OUT)
    print(f"  -> {OUT} ({n} baris, termasuk 1 baris live). Kolom baru: {sn_cols+sn_btc_cols}. Upload ke Claude.")

if __name__=="__main__":
    try:
        main()
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"\n[FATAL] build gagal: {type(e).__name__}: {e}")
        print("[INFO] CSV lama TIDAK diubah; run_local.bat (versi baru) tidak akan commit/push.")
        sys.exit(1)
