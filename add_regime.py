#!/usr/bin/env python3
"""
add_regime.py — Regime classifier untuk S7 MC pipeline.
Output per-bar: UP / DOWN / CHOP  (+ ma200, ma200_slope, adx, plus_di, minus_di)

Prinsip:
  - NO-LOOKAHEAD: tiap nilai bar-t hanya pakai data <= t (causal).
  - STDLIB-ONLY (konsisten add_signals.py / add_volume_profile.py).
  - Transparan (rule-based, bukan ML) — bisa diaudit & di-backtest per-rezim.

Logika (dari walk-forward findings kamu: MA200+slope & ADX>20 outperform base):
  ADX < ADX_TREND            -> CHOP  (tren lemah, range)
  price>MA200 & slope>+thr   -> UP
  price<MA200 & slope<-thr   -> DOWN
  selain itu                 -> CHOP  (transisi / mixed)

Pakai:
  python add_regime.py                      # in-place append ke CSV default
  python add_regime.py in.csv out.csv       # baca in.csv, tulis out.csv
"""
import csv, sys, math, os

# ---- PARAM (tunable) ----
BASE    = os.path.dirname(os.path.abspath(__file__))   # folder tempat skrip berada
CSV_IN  = sys.argv[1] if len(sys.argv) > 1 else os.path.join(BASE, "btc_merged_hourly.csv")
CSV_OUT = sys.argv[2] if len(sys.argv) > 2 else CSV_IN   # default: overwrite in-place
MA_LEN        = 200     # MA200 (jam) ~ 8.3 hari
SLOPE_LB      = 24      # slope MA diukur atas 24 bar
SLOPE_THRESH  = 0.0015  # 0.15% per 24h = ambang arah (tunable)
ADX_LEN       = 14
ADX_TREND     = 20      # ADX>20 = trending (registry finding)

NEW_COLS = ["ma200", "ma200_slope", "adx", "plus_di", "minus_di", "regime"]

# ---------- load ----------
with open(CSV_IN, newline="") as f:
    reader = csv.reader(f)
    header = next(reader)
    rows = [r for r in reader]

idx = {c: i for i, c in enumerate(header)}
def g(row, col):
    v = row[idx[col]] if idx[col] < len(row) else ""
    try: return float(v)
    except (ValueError, TypeError): return None

highs  = [g(r, "high")  for r in rows]
lows   = [g(r, "low")   for r in rows]
closes = [g(r, "close") for r in rows]
n = len(rows)

# ---------- MA200 (causal rolling mean) ----------
ma200 = [None]*n
csum = 0.0; window = []
for i in range(n):
    c = closes[i]
    if c is None:            # jaga-jaga gap
        c = closes[i-1] if i>0 and closes[i-1] is not None else 0.0
    window.append(c); csum += c
    if len(window) > MA_LEN:
        csum -= window.pop(0)
    if len(window) == MA_LEN:
        ma200[i] = csum / MA_LEN

# ---------- slope MA200 (past-only) ----------
slope = [None]*n
for i in range(n):
    if ma200[i] is not None and i-SLOPE_LB >= 0 and ma200[i-SLOPE_LB] is not None and ma200[i-SLOPE_LB] != 0:
        slope[i] = (ma200[i] - ma200[i-SLOPE_LB]) / ma200[i-SLOPE_LB]

# ---------- ADX(14) Wilder (causal) ----------
plus_di  = [None]*n
minus_di = [None]*n
adx      = [None]*n
tr_s = pdm_s = ndm_s = None   # smoothed
dx_hist = []
for i in range(1, n):
    h, l, pc = highs[i], lows[i], closes[i-1]
    ph, pl = highs[i-1], lows[i-1]
    if None in (h, l, pc, ph, pl):
        continue
    tr = max(h-l, abs(h-pc), abs(l-pc))
    up_move, dn_move = h-ph, pl-l
    pdm = up_move if (up_move > dn_move and up_move > 0) else 0.0
    ndm = dn_move if (dn_move > up_move and dn_move > 0) else 0.0
    if tr_s is None:                       # seed
        tr_s, pdm_s, ndm_s = tr, pdm, ndm
    else:                                  # Wilder smoothing
        tr_s  = tr_s  - tr_s/ADX_LEN  + tr
        pdm_s = pdm_s - pdm_s/ADX_LEN + pdm
        ndm_s = ndm_s - ndm_s/ADX_LEN + ndm
    if tr_s > 0:
        pdi = 100*pdm_s/tr_s
        ndi = 100*ndm_s/tr_s
        plus_di[i], minus_di[i] = pdi, ndi
        denom = pdi+ndi
        dx = 100*abs(pdi-ndi)/denom if denom>0 else 0.0
        dx_hist.append(dx)
        if len(dx_hist) >= ADX_LEN:
            if adx[i-1] is None:
                adx[i] = sum(dx_hist[-ADX_LEN:])/ADX_LEN     # first ADX = SMA of DX
            else:
                adx[i] = (adx[i-1]*(ADX_LEN-1) + dx)/ADX_LEN # Wilder

# ---------- regime label ----------
regime = [None]*n
for i in range(n):
    a, s, m, c = adx[i], slope[i], ma200[i], closes[i]
    if None in (a, s, m, c):
        regime[i] = "WARMUP"
    elif a < ADX_TREND:
        regime[i] = "CHOP"
    elif c > m and s > SLOPE_THRESH:
        regime[i] = "UP"
    elif c < m and s < -SLOPE_THRESH:
        regime[i] = "DOWN"
    else:
        regime[i] = "CHOP"

# ---------- write ----------
def fmt(v): return "" if v is None else (f"{v:.6f}" if isinstance(v, float) else str(v))
out_header = header + [c for c in NEW_COLS if c not in header]
with open(CSV_OUT, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(out_header)
    for i, r in enumerate(rows):
        w.writerow(r + [fmt(ma200[i]), fmt(slope[i]), fmt(adx[i]),
                         fmt(plus_di[i]), fmt(minus_di[i]), regime[i]])

# ---------- ringkasan ----------
from collections import Counter
cnt = Counter(regime)
print("="*56)
print(f"add_regime.py  |  {n} bar  ->  {CSV_OUT}")
print("="*56)
for k in ["UP","DOWN","CHOP","WARMUP"]:
    if cnt[k]: print(f"  {k:6}: {cnt[k]:5}  ({cnt[k]/n*100:4.1f}%)")
# baris live terakhir
last = n-1
print(f"\nBAR TERAKHIR ({rows[last][idx['timestamp_utc']]}):")
print(f"  close={closes[last]}  MA200={fmt(ma200[last])}  slope={fmt(slope[last])}")
print(f"  ADX={fmt(adx[last])}  +DI={fmt(plus_di[last])}  -DI={fmt(minus_di[last])}")
print(f"  >>> REGIME = {regime[last]}")
