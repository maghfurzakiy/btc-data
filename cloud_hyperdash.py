#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CLOUD collector (GitHub Actions) — HANYA HyperDash.
Binance fapi DIBLOKIR dari IP GitHub (HTTP 451 geo-block AS). Maka cloud fokus pada
SATU hal yang butuh akumulasi 24/7: snapshot UPNL HyperDash.
Append ke upnl_history.csv (skema sama dgn build_merged_dataset.py).
Harga BTC diambil dari Coinbase (geo-netral) sekadar konteks. Binance + merge = dikerjakan lokal.
"""
import json,csv,os,urllib.request
from datetime import datetime,timezone

BASE=os.path.dirname(os.path.abspath(__file__)); ARCH=os.path.join(BASE,"upnl_history.csv")
HD="https://api.hyperdash.com/graphql"
def hd_post(op,vars,q):
    body=json.dumps({"operationName":op,"variables":vars,"query":q}).encode()
    req=urllib.request.Request(HD,data=body,headers={"Content-Type":"application/json","Origin":"https://hyperdash.com","User-Agent":"Mozilla/5.0"},method="POST")
    with urllib.request.urlopen(req,timeout=60) as r: return json.loads(r.read().decode())["data"]

def get_price():
    try:
        req=urllib.request.Request("https://api.coinbase.com/v2/prices/BTC-USD/spot",headers={"User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req,timeout=30) as r: return round(float(json.loads(r.read().decode())["data"]["amount"]),1)
    except Exception as e:
        print("  harga Coinbase gagal:",e); return ""

def test_positioning():
    q="query HistoricalCohortPositioningV2($startTime: Float!){analytics{historicalCohortPositioningV2(startTime:$startTime){timestamp positioning __typename}__typename}}"
    try:
        pts=hd_post("HistoricalCohortPositioningV2",{"startTime":1672531200000},q)["analytics"]["historicalCohortPositioningV2"]
        print(f"  HyperDash positioning: OK ({len(pts)} entri)"); return True
    except Exception as e:
        print(f"  HyperDash positioning: GAGAL -> {type(e).__name__}: {e}"); return False

def capture_upnl():
    q="query GetPnlCohort($id: String!){analytics{pnlCohort(id:$id){profitTraders lossTraders __typename}__typename}}"
    def pp(cid):
        d=hd_post("GetPnlCohort",{"id":cid},q)["analytics"]["pnlCohort"]; p,l=d.get("profitTraders") or 0,d.get("lossTraders") or 0
        return round(p/(p+l)*100,1) if (p+l)>0 else ""
    return pp("extremely_profitable"),pp("very_unprofitable")

def update_archive(ts,price,ep,vu):
    rows={}
    if os.path.exists(ARCH):
        with open(ARCH,encoding="utf-8") as f:
            for r in csv.reader(f):
                if r and r[0]!="snapshot_ts": rows[int(r[0])//60*60]=r
    rows[ts//60*60]=[ts,"cloud",price,ep,vu]
    ordered=[rows[k] for k in sorted(rows)]
    with open(ARCH,"w",newline="",encoding="utf-8") as f:
        w=csv.writer(f); w.writerow(["snapshot_ts","source","btc_price","ep_profPct","vu_profPct"]); w.writerows(ordered)
    return len(ordered)

def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] cloud HyperDash collector ...")
    test_positioning()
    try:
        ep,vu=capture_upnl(); price=get_price(); ts=int(datetime.now(timezone.utc).timestamp())
        n=update_archive(ts,price,ep,vu)
        print(f"  UPNL snapshot: ep={ep} vu={vu} price={price} -> arsip {n} baris")
        print("  >>> HASIL: HyperDash JALAN dari cloud. Cloud bisa jadi backbone UPNL 24/7.")
    except Exception as e:
        print(f"  UPNL: GAGAL -> {type(e).__name__}: {e}")
        print("  >>> HASIL: HyperDash DIBLOKIR dari cloud. Perlu pivot (kolektor rumah / lokal-saja).")

if __name__=="__main__": main()
