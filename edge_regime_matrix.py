#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
edge_regime_matrix.py — RESEARCH tool (BUKAN pipeline enricher).

Uji tiap edge tervalidasi, DI-PECAH per-rezim (UP/DOWN/CHOP), no-lookahead.
Bandingkan edge vs BASELINE rezim (semua bar di rezim itu) -> "lift".
Jalankan STANDALONE, sesekali (saat rezim ganti / edge baru). Hasilnya = tabel
gating: edge mana ON di rezim mana. Encode temuannya ke framework, JANGAN taruh di run_local.bat.

Prasyarat: CSV sudah punya kolom 'regime' (jalankan add_regime.py dulu).
Pakai: python edge_regime_matrix.py [csv_with_regime]
"""
import csv, sys, math

PATH = sys.argv[1] if len(sys.argv) > 1 else "work.csv"
MIN_N = 30   # skip sel < 30 sampel

rows = list(csv.DictReader(open(PATH)))
def f(r, c):
    try: return float(r[c])
    except (ValueError, KeyError, TypeError): return None

close = [f(r, "close") for r in rows]
n = len(rows)

def fwd_ret(i, h, direction):
    if i + h >= n: return None
    c0, cH = close[i], close[i+h]
    if c0 is None or cH is None or c0 <= 0: return None
    r = (cH - c0) / c0
    return r * direction   # short: direction=-1

# --- definisi edge: (nama, fungsi_sinyal, arah, horizon) ---
def ep33(r):   v=f(r,"ep_btc");  return v is not None and v<=33
def ep25(r):   v=f(r,"ep_btc");  return v is not None and v<=25
def vaf_lo(r): return r.get("vp96_pos")=="below_val"
def vaf_hi(r): return r.get("vp96_pos")=="above_vah"
def bamp(r):
    e=f(r,"ep_btc"); b=f(r,"basis_pct")
    return e is not None and e<=33 and b is not None and b<-0.06

EDGES = [
    ("ep33_long",      ep33,   +1, 24),
    ("ep25deep_long",  ep25,   +1, 24),
    ("basisamp_long",  bamp,   +1, 24),
    ("vafade96_long",  vaf_lo, +1, 48),
    ("vafade96_short", vaf_hi, -1, 48),
]
REGIMES = ["UP", "DOWN", "CHOP"]

def stats(rets):
    if not rets: return (0, 0.0, 0.0)
    wr = sum(1 for x in rets if x>0)/len(rets)*100
    mn = sum(rets)/len(rets)*100
    return (len(rets), wr, mn)

print("="*74)
print("EDGE × REGIME MATRIX  |  no-lookahead  |  edge vs baseline-rezim (lift)")
print("="*74)

for name, sig, dirn, H in EDGES:
    print(f"\n### {name}  (arah={'LONG' if dirn>0 else 'SHORT'}, fwd={H}h)")
    print(f"{'regime':7}{'n':>5}{'edgeWR':>8}{'edgeRet':>9}{'baseWR':>8}{'baseRet':>9}{'lift':>8}")
    for reg in REGIMES:
        edge_r, base_r = [], []
        for i in range(n-H):
            if rows[i].get("regime") != reg: continue
            r = fwd_ret(i, H, dirn)
            if r is None: continue
            base_r.append(r)                 # semua bar di rezim (arah sama)
            if sig(rows[i]): edge_r.append(r) # bar yg sinyal aktif
        en, ewr, emn = stats(edge_r)
        bn, bwr, bmn = stats(base_r)
        if en < MIN_N:
            print(f"{reg:7}{en:>5}   (sampel<{MIN_N}, skip)")
            continue
        lift = emn - bmn
        flag = "  <<<" if (ewr>=55 and lift>0.10) else ("  xx" if ewr<48 else "")
        print(f"{reg:7}{en:>5}{ewr:>7.1f}%{emn:>+8.3f}{bwr:>7.1f}%{bmn:>+8.3f}{lift:>+7.3f}{flag}")

print("\n" + "="*74)
print("<<< = edge ON di rezim ini (WR>=55 & lift>+0.10%)   xx = edge RUGI (WR<48)")
print("Semua hypothesis-grade; confound single-regime. Re-run saat rezim ganti.")
print("="*74)
