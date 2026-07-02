#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
add_signals.py — tambah kolom SINYAL Tier-1 ke btc_merged_hourly.csv (Batch A backlog).

POST-PROCESSOR (pola sama add_long_notional.py / add_volume_profile.py):
baca CSV yang sudah ada -> hitung NO-LOOKAHEAD per baris (cuma pakai data <= baris itu) ->
APPEND kolom di AKHIR skema (urutan kolom lama utuh) -> atomic write (.tmp->replace) ->
idempoten (overwrite kalau kolom sudah ada). HANYA pustaka standar. NOL network (CSV-only).
Aman ditaruh di run_local.bat SETELAH build (urutan bebas vs add_long_notional/vp_).

=== KOLOM BARU ===
  tz        : leverage z-score. z dari total-notional (ep_sn + ep_ln) atas window 72h.
              + = leverage di atas normal (bahan squeeze/flush). Gate LONG-scalp: tz in [-0,5; 0,5].
  er        : Kaufman Efficiency Ratio (close, n=10). [0..1]. <~0,4 = choppy/range, >~0,6 = trending.
              Gate regime: aktifkan strategi fade VA-edge cuma saat er rendah (range).
  ecc       : state kategorikal ECC, CERMIN ecc_detector.py:
              standaside | anti | fire | partial | watch | na
  ecc_d1    : ep_btc Δ3h (pp)        [numerik -> backtest bisa sweep ambang leg-1, default +2,0]
  ecc_d3    : ep_profPct Δ3h          [numerik -> leg-3 / deteksi anti (Δ>0)]
  rv24      : realized-vol annualized % atas 24h (dari log-return close).
  rv96      : realized-vol annualized % atas 96h (swing).
  rv_ts     : term-structure rv24/rv96. >1 = vol jangka-pendek elevated (sinkron buat divergence vs IV).

=== ALASAN DESAIN ===
  - ECC cermin DETEKTOR: definisi & ambang diimpor satu sumber (LEVEL_ARM=33, LEG1_PP=2,0) supaya
    kolom == output ecc_detector.py. Tidak ada definisi ganda.
  - ecc_d1/ecc_d3 NUMERIK sengaja: biar backtest sweep ambang, bukan hard-code (disiplin moonWatch).
  - rv24/rv96 (bukan rv6): 6 return per-jam terlalu berisik. 24h/96h stabil & punya pertanyaan
    backtest nyata (regime vol + divergence IV-RV). Ganti window via konstanta kalau perlu.

PEMAKAIAN
  python add_signals.py                          # default: btc_merged_hourly.csv di folder ini
  python add_signals.py btc_merged_hourly.csv    # path eksplisit
  python add_signals.py --selftest               # uji logika tanpa baca CSV besar
"""
import csv, os, sys, math

BASE = os.path.dirname(os.path.abspath(__file__))
CSV  = sys.argv[1] if len(sys.argv) > 1 else os.path.join(BASE, "btc_merged_hourly.csv")

# ---------------- AMBANG / KONFIG (samakan dgn ecc_detector.py) ----------------
LEVEL_ARM = 33.0      # gate contrarian-long (ep_btc <= 33)
LEG1_PP   = 2.0       # leg-1: ep_btc naik >= +2,0pp / 3h
TZ_WIN    = 72        # window z-score leverage (jam)
TZ_MIN    = 72        # minimum nilai valid utk emit tz
ER_N      = 10        # window efficiency ratio (close)
RV_SHORT  = 24        # window realized-vol pendek (jam)
RV_LONG   = 96        # window realized-vol panjang (jam)
ANN       = math.sqrt(24 * 365)   # annualisasi hourly -> ~93,6

NEW_COLS = ["tz", "er", "ecc", "ecc_d1", "ecc_d3", "rv24", "rv96", "rv_ts"]

# ---------------- util ----------------
def fnum(x):
    try:
        v = float(x)
        return v if v == v else None   # buang NaN
    except (TypeError, ValueError):
        return None

def trailing_valid(arr, i, n):
    """n nilai valid terakhir (None dilewati) s/d indeks i, urut lama->baru."""
    out = []
    j = i
    while j >= 0 and len(out) < n:
        v = arr[j]
        if v is not None:
            out.append(v)
        j -= 1
    return out[::-1]

def zscore(window):
    """z-score nilai terakhir vs window (sample std, ddof=1). '' kalau tak cukup/var nol."""
    n = len(window)
    if n < 2:
        return ""
    mu = sum(window) / n
    var = sum((x - mu) ** 2 for x in window) / (n - 1)
    if var <= 0:
        return ""
    return round((window[-1] - mu) / math.sqrt(var), 2)

def efficiency_ratio(closes):
    """Kaufman ER atas closes (urut lama->baru, len = ER_N+1)."""
    if len(closes) < 2:
        return ""
    change = abs(closes[-1] - closes[0])
    vol = sum(abs(closes[k] - closes[k - 1]) for k in range(1, len(closes)))
    if vol <= 0:
        return ""
    return round(change / vol, 3)

def realized_vol(closes):
    """annualized realized vol % dari log-return closes (urut lama->baru)."""
    if len(closes) < 3:
        return ""
    rets = []
    for k in range(1, len(closes)):
        a, b = closes[k - 1], closes[k]
        if a and a > 0 and b and b > 0:
            rets.append(math.log(b / a))
    if len(rets) < 2:
        return ""
    mu = sum(rets) / len(rets)
    var = sum((r - mu) ** 2 for r in rets) / (len(rets) - 1)
    return round(math.sqrt(var) * ANN * 100.0, 2)

def ecc_state(epb_h, prof_h, sn_h):
    """state ECC + (d1,d3). Cermin verdict ecc_detector.py.
       epb_h/prof_h: list valid lama->baru (<=4). sn_h: list valid (<=4) atau None bila kolom absen."""
    cur = epb_h[-1] if epb_h else None
    if cur is None:
        return ("na", "", "")
    gate = cur <= LEVEL_ARM

    # leg-1: ep_btc Δ3h
    d1 = None
    if len(epb_h) >= 4:   d1 = epb_h[-1] - epb_h[-4]
    elif len(epb_h) >= 2: d1 = epb_h[-1] - epb_h[0]
    leg1 = d1 is not None and d1 >= LEG1_PP

    # leg-3: ep_profPct Δ3h (cover = turun)
    d3 = None
    if len(prof_h) >= 4:   d3 = prof_h[-1] - prof_h[-4]
    elif len(prof_h) >= 2: d3 = prof_h[-1] - prof_h[0]
    leg3 = d3 is not None and d3 < 0
    anti = d3 is not None and d3 > 0   # profPct NAIK = short makin in-profit = belum cover

    # leg-2: ep_btc_sn turun 3 jam beruntun (butuh 4 nilai)
    if sn_h is None:
        leg2_known, leg2 = False, False
    elif len(sn_h) >= 4:
        leg2_known, leg2 = True, (sn_h[-1] < sn_h[-2] < sn_h[-3] < sn_h[-4])
    else:
        leg2_known, leg2 = False, False

    if not gate:
        state = "standaside"
    elif anti:
        state = "anti"
    elif leg2_known and leg1 and leg2 and leg3:
        state = "fire"
    elif leg1 and leg3:          # leg-2 absen/false tapi timing lain on
        state = "partial"
    else:
        state = "watch"

    return (state,
            "" if d1 is None else round(d1, 2),
            "" if d3 is None else round(d3, 1))

# ---------------- main ----------------
def main(path):
    if not os.path.exists(path):
        print(f"[ERR] file tidak ada: {path}"); sys.exit(1)

    with open(path, encoding="utf-8", newline="") as f:
        rd = csv.reader(f); header = next(rd); rows = list(rd)
    idx = {c: i for i, c in enumerate(header)}

    for need in ("close", "ep_btc", "ep_profPct"):
        if need not in idx:
            print(f"[ERR] kolom '{need}' tak ada — bukan CSV builder."); sys.exit(1)
    has_sn = ("ep_sn" in idx and "ep_ln" in idx)
    has_btc_sn = "ep_btc_sn" in idx
    if not has_sn:
        print("[WARN] ep_sn/ep_ln absen -> tz kosong (jalankan builder patch _sn).")
    if not has_btc_sn:
        print("[WARN] ep_btc_sn absen -> ECC leg-2 tak dinilai (state max 'partial').")

    header_out = header + [c for c in NEW_COLS if c not in idx]
    out_idx = {c: i for i, c in enumerate(header_out)}

    # array kolom (None utk kosong)
    close = [fnum(r[idx["close"]]) if idx["close"] < len(r) else None for r in rows]
    epb   = [fnum(r[idx["ep_btc"]]) if idx["ep_btc"] < len(r) else None for r in rows]
    prof  = [fnum(r[idx["ep_profPct"]]) if idx["ep_profPct"] < len(r) else None for r in rows]
    if has_sn:
        tot = []
        for r in rows:
            s = fnum(r[idx["ep_sn"]]) if idx["ep_sn"] < len(r) else None
            l = fnum(r[idx["ep_ln"]]) if idx["ep_ln"] < len(r) else None
            tot.append((s + l) if (s is not None and l is not None) else None)
    else:
        tot = [None] * len(rows)
    sn = ([fnum(r[idx["ep_btc_sn"]]) if idx["ep_btc_sn"] < len(r) else None for r in rows]
          if has_btc_sn else None)

    fill = {c: 0 for c in NEW_COLS}
    out_rows = []
    for i, r in enumerate(rows):
        r = list(r) + [""] * (len(header_out) - len(r))

        # tz
        tz = ""
        if has_sn:
            w = trailing_valid(tot, i, TZ_WIN)
            if len(w) >= TZ_MIN:
                tz = zscore(w)
        r[out_idx["tz"]] = tz

        # er
        er = efficiency_ratio(trailing_valid(close, i, ER_N + 1))
        r[out_idx["er"]] = er

        # ecc
        sn_h = trailing_valid(sn, i, 4) if sn is not None else None
        state, d1, d3 = ecc_state(trailing_valid(epb, i, 4),
                                  trailing_valid(prof, i, 4), sn_h)
        r[out_idx["ecc"]] = state
        r[out_idx["ecc_d1"]] = d1
        r[out_idx["ecc_d3"]] = d3

        # rv
        rv24 = realized_vol(trailing_valid(close, i, RV_SHORT + 1))
        rv96 = realized_vol(trailing_valid(close, i, RV_LONG + 1))
        r[out_idx["rv24"]] = rv24
        r[out_idx["rv96"]] = rv96
        r[out_idx["rv_ts"]] = (round(rv24 / rv96, 3)
                               if (rv24 != "" and rv96 not in ("", 0)) else "")

        for c in NEW_COLS:
            if r[out_idx[c]] not in ("", None):
                fill[c] += 1
        out_rows.append(r)

    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(header_out); w.writerows(out_rows)
    os.replace(tmp, path)

    appended = [c for c in NEW_COLS if c not in idx]
    print(f"[OK] {path}")
    print(f"     kolom: {'+'.join(appended) if appended else '(sudah ada, overwrite)'}  | baris: {len(out_rows)}")
    for c in NEW_COLS:
        print(f"     {c:8s} terisi {fill[c]}/{len(out_rows)}")
    if out_rows:
        last = out_rows[-1]; g = lambda c: last[out_idx[c]]
        print(f"     baris akhir: tz {g('tz')}  er {g('er')}  ecc {g('ecc')} "
              f"(d1 {g('ecc_d1')} d3 {g('ecc_d3')})  rv24 {g('rv24')}  rv96 {g('rv96')}  rv_ts {g('rv_ts')}")

# ---------------- selftest (tanpa CSV besar) ----------------
def selftest():
    print("=== SELFTEST add_signals ===")
    # zscore
    z = zscore([1, 2, 3, 4, 10]); print(f" zscore[..,10]={z} (harap > 1.5)")
    # ER: monoton naik = efisien (~1)
    er1 = efficiency_ratio([100, 101, 102, 103, 104]); print(f" ER monoton={er1} (harap 1.0)")
    er2 = efficiency_ratio([100, 101, 100, 101, 100]); print(f" ER zigzag={er2} (harap rendah)")
    # ECC: fire scenario
    epb = [20, 21, 22, 24]                 # +4pp/3h -> leg1 ON, gate ON
    prof = [70, 68, 66, 64]                # turun -> leg3 ON
    sn = [400, 390, 380, 370]              # turun beruntun -> leg2 ON
    s, d1, d3 = ecc_state(epb, prof, sn); print(f" ECC fire-case -> {s} d1={d1} d3={d3} (harap fire)")
    # ECC: anti (profPct naik)
    s2, *_ = ecc_state([22, 22, 22, 22], [60, 62, 64, 66], [400, 401, 402, 403]); print(f" ECC anti-case -> {s2} (harap anti)")
    # ECC: watch (gate on, timing flat, sn loaded)
    s3, _, d3b = ecc_state([22.9, 24.3, 24.6, 22.9], [63.6, 63.6, 63.6, 63.6], [395, 392, 394, 395])
    print(f" ECC watch-case -> {s3} d3={d3b} (harap watch)")
    # ECC: standaside (ep_btc > 33)
    s4, *_ = ecc_state([40, 41, 42, 43], [60, 59, 58, 57], [400, 390, 380, 370]); print(f" ECC ep>33 -> {s4} (harap standaside)")
    # rv
    rv = realized_vol([60000, 60300, 59900, 60100, 60000]); print(f" rv sample={rv}% (annualized)")
    ok = (s == "fire" and s2 == "anti" and s3 == "watch" and s4 == "standaside" and er1 == 1.0)
    print(" RESULT:", "OK ✅" if ok else "CEK ❌")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest(); sys.exit(0)
    try:
        main(CSV)
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"\n[FATAL] {type(e).__name__}: {e}\n[INFO] CSV lama TIDAK diubah (atomic .tmp).")
        sys.exit(1)
