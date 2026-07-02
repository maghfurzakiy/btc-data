#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ECC DETECTOR — baca btc_merged_hourly.csv, lapor status sinyal ECC (3-leg) + gate LEVEL.

Definisi (sesuai journal S7 MC):
  GATE LEVEL  : ep_btc <= 33 (edge contrarian-long tervalidasi; ideal/deep <= 25).
  LEG-1 cohort: ep_btc NAIK >= +2,0pp dalam 3 jam (cohort flip dari short -> mulai cover).
  LEG-2 notion: ep_btc_sn (short-notional $) TURUN 3 jam beruntun (short ditutup).   [butuh patch *_sn]
  LEG-3 uPNL  : ep_profPct TURUN dalam 3 jam (short-UPNL mengkerut = profit short hilang).

  ECC FIRE     = GATE LEVEL ON  &  LEG-1 & LEG-2 & LEG-3 semua ON.
  ANTI-ECC     = ep_profPct NAIK (short makin in-profit = belum cover) -> bias FADE/akumulasi-sabar.
  PARTIAL      = sebagian leg ON.

Catatan: leg-2 hanya bisa dinilai kalau CSV punya kolom ep_btc_sn (hasil build_merged_dataset.py
versi patch). Kalau belum ada -> dilaporkan 'n/a (kolom ep_btc_sn belum ada — pakai builder patch)'.
"""
import csv,sys,os

CSV=sys.argv[1] if len(sys.argv)>1 else "btc_merged_hourly.csv"
LEVEL_ARM=33.0; LEVEL_DEEP=25.0; LEG1_PP=2.0   # ambang

def f(x):
    try: return float(x)
    except: return None

def load_hourly(path):
    rows=[r for r in csv.DictReader(open(path,encoding="utf-8")) if r.get("source")=="hourly"]
    live=[r for r in csv.DictReader(open(path,encoding="utf-8")) if r.get("source")=="live"]
    return rows, (live[-1] if live else None)

def last_vals(rows, key, n):
    """n nilai terakhir non-kosong utk key, urut lama->baru (None kalau kolom tak ada)."""
    if rows and key not in rows[0]: return None
    out=[]
    for r in rows:
        v=f(r.get(key,""))
        out.append(v)
    # ambil n terakhir yang ada nilainya, jaga urutan
    return [v for v in out if v is not None][-n:]

def main():
    if not os.path.exists(CSV):
        print(f"[ERR] file tidak ada: {CSV}"); sys.exit(1)
    rows,live=load_hourly(CSV)
    if len(rows)<4:
        print("[ERR] butuh >=4 baris hourly."); sys.exit(1)
    ts=rows[-1]["timestamp_utc"]; close=f(rows[-1].get("close"))

    epb=last_vals(rows,"ep_btc",4)
    prof=last_vals(rows,"ep_profPct",4)
    sn=last_vals(rows,"ep_btc_sn",4)   # None kalau kolom belum ada (builder lama)

    cur_epb=epb[-1] if epb else None
    cur_prof=prof[-1] if prof else None

    # GATE LEVEL
    gate = cur_epb is not None and cur_epb<=LEVEL_ARM
    deep = cur_epb is not None and cur_epb<=LEVEL_DEEP

    # LEG-1: ep_btc naik >= +2pp dlm 3 jam (bandingkan vs 3 langkah lalu)
    leg1=None
    if epb and len(epb)>=4:
        d3=epb[-1]-epb[-4]; leg1=(d3>=LEG1_PP, round(d3,2))
    elif epb and len(epb)>=2:
        d=epb[-1]-epb[0]; leg1=(d>=LEG1_PP, round(d,2))

    # LEG-2: ep_btc_sn turun 3 jam beruntun
    if sn is None:
        leg2=("n/a","kolom ep_btc_sn belum ada — pakai builder patch")
    elif len(sn)>=4:
        dec = sn[-1]<sn[-2]<sn[-3]<sn[-4]
        leg2=(dec, f"{sn[-4]:.0f}->{sn[-3]:.0f}->{sn[-2]:.0f}->{sn[-1]:.0f}")
    else:
        leg2=("n/a","data ep_btc_sn < 4 baris")

    # LEG-3: ep_profPct turun dlm 3 jam
    leg3=None
    if prof and len(prof)>=4:
        d3=prof[-1]-prof[-4]; leg3=(d3<0, round(d3,1))
    elif prof and len(prof)>=2:
        d=prof[-1]-prof[0]; leg3=(d<0, round(d,1))

    # anti-ECC: ep_profPct NAIK
    anti = leg3 is not None and leg3[1] is not None and leg3[1]>0

    def mark(b): return "✅" if b is True else ("❌" if b is False else "▫️")

    print("="*64)
    print(f"ECC DETECTOR  |  {ts}  |  close {close}")
    print("="*64)
    print(f"ep_btc        : {cur_epb}   GATE<=33 {mark(gate)}  DEEP<=25 {mark(deep)}")
    print(f"ep_profPct    : {cur_prof}")
    print("-"*64)
    if leg1: print(f"LEG-1 cohort  : {mark(leg1[0])}  ep_btc Δ3h = {leg1[1]:+} pp   (butuh >= +{LEG1_PP})")
    else:    print(f"LEG-1 cohort  : ▫️  data kurang")
    if leg2[0]=="n/a": print(f"LEG-2 notion  : ▫️  {leg2[1]}")
    else:              print(f"LEG-2 notion  : {mark(leg2[0])}  ep_btc_sn {leg2[1]} (turun beruntun?)")
    if leg3: print(f"LEG-3 uPNL    : {mark(leg3[0])}  ep_profPct Δ3h = {leg3[1]:+}   (cover = TURUN)")
    else:    print(f"LEG-3 uPNL    : ▫️  data kurang")
    print("-"*64)

    legs_known=[l for l in [leg1[0] if leg1 else None,
                            (leg2[0] if leg2[0]!="n/a" else None),
                            leg3[0] if leg3 else None] if l is not None]
    all_on = gate and len(legs_known)>0 and all(legs_known) and (leg2[0] is True or leg2[0]=="n/a")
    # ECC penuh hanya valid kalau leg-2 benar2 dinilai
    full_ready = leg2[0] in (True,False)

    if not gate:
        verdict="STAND-ASIDE LEVEL (ep_btc > 33, edge contrarian-long OFF)"
    elif anti:
        verdict="ANTI-ECC (ep_profPct NAIK = short in-profit, belum cover) -> FADE / akumulasi sabar"
    elif gate and full_ready and leg1 and leg1[0] and leg2[0] and leg3 and leg3[0]:
        verdict="🔥 ECC FIRE (3-leg + gate) -> long-contrarian armed PENUH"
    elif gate and (leg1 and leg1[0]) and (leg3 and leg3[0]):
        verdict="PARTIAL (gate+leg1+leg3 ON; leg-2 belum bisa dinilai -> pakai builder patch utk konfirmasi penuh)"
    else:
        verdict="GATE ON, leg belum lengkap -> WATCH (level-edge aktif, timing belum)"
    print(f"VERDICT       : {verdict}")
    if live:
        print(f"(live row     : ep_btc {live.get('ep_btc')}  ep_profPct {live.get('ep_profPct')}  close {live.get('close')})")
    print("="*64)

if __name__=="__main__":
    main()
