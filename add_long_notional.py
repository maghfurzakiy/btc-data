#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
add_long_notional.py — tambah kolom LONG-notional ke btc_merged_hourly.csv TANPA narik ulang.

Identitas aljabar (exact, bukan aproksimasi):
    ep_btc  = 100 * longN / (longN + shortN)        ->  long% cohort BTC
    ep_btc_sn = shortN ($)                            ->  sudah ada di CSV (patch 1.43)
  =>  longN = shortN * ep_btc / (100 - ep_btc)
  =>  ep_btc_ln = ep_btc_sn * ep_btc / (100 - ep_btc)
  Sama utk agregat: ep_ln = ep_sn * ep / (100 - ep)

Menambah 2 kolom di AKHIR skema (urutan kolom lama utuh): ep_ln, ep_btc_ln.
Idempoten: kalau kolom sudah ada -> dihitung ulang (overwrite), aman dijalankan berkali-kali.
Atomic write (.tmp -> replace). Hanya pustaka standar.

Pakai:  python add_long_notional.py [path_csv]   (default: btc_merged_hourly.csv di folder ini)
"""
import csv, os, sys

BASE = os.path.dirname(os.path.abspath(__file__))
CSV  = sys.argv[1] if len(sys.argv) > 1 else os.path.join(BASE, "btc_merged_hourly.csv")

# pasangan: (kolom_long_baru, kolom_pct_long, kolom_short_notional)
PAIRS = [("ep_ln", "ep", "ep_sn"),
         ("ep_btc_ln", "ep_btc", "ep_btc_sn")]

def fnum(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def long_notional(pct, short_n):
    """longN = shortN * pct/(100-pct). Kosong kalau data tak lengkap / pct di luar (0,100)."""
    p, s = fnum(pct), fnum(short_n)
    if p is None or s is None:
        return ""
    if p <= 0:                 # tidak ada long-side (semua short)
        return 0.0 if s >= 0 else ""
    if p >= 100:               # tidak terdefinisi (tak ada short utk dibagi)
        return ""
    return round(s * p / (100.0 - p), 0)

def main():
    if not os.path.exists(CSV):
        print(f"[ERR] file tidak ada: {CSV}"); sys.exit(1)

    with open(CSV, encoding="utf-8", newline="") as f:
        rd = csv.reader(f)
        header = next(rd)
        rows = list(rd)

    idx = {c: i for i, c in enumerate(header)}
    # validasi kolom sumber
    for _, pct_c, sn_c in PAIRS:
        for need in (pct_c, sn_c):
            if need not in idx:
                print(f"[ERR] kolom sumber '{need}' tak ada — jalankan builder versi patch _sn dulu."); sys.exit(1)

    # siapkan kolom output (append di akhir bila belum ada; overwrite bila ada)
    new_cols = [ln_c for ln_c, _, _ in PAIRS if ln_c not in idx]
    header_out = header + new_cols
    out_idx = {c: i for i, c in enumerate(header_out)}

    filled = {ln_c: 0 for ln_c, _, _ in PAIRS}
    out_rows = []
    for r in rows:
        r = list(r) + [""] * (len(header_out) - len(r))   # rapikan panjang
        for ln_c, pct_c, sn_c in PAIRS:
            val = long_notional(r[idx[pct_c]], r[idx[sn_c]])
            r[out_idx[ln_c]] = "" if val == "" else (int(val) if float(val).is_integer() else val)
            if val != "":
                filled[ln_c] += 1
        out_rows.append(r)

    tmp = CSV + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header_out)
        w.writerows(out_rows)
    os.replace(tmp, CSV)

    print(f"[OK] {CSV}")
    print(f"     kolom: {'+'.join(new_cols) if new_cols else '(sudah ada, di-overwrite)'}  | baris: {len(out_rows)}")
    for ln_c, _, _ in PAIRS:
        print(f"     {ln_c:10s} terisi {filled[ln_c]}/{len(out_rows)}")
    # sampel nilai terakhir
    if out_rows:
        last = out_rows[-1]
        def g(c): return last[out_idx[c]] if c in out_idx else "?"
        print(f"     contoh baris akhir: ep_btc {g('ep_btc')}  short ep_btc_sn {g('ep_btc_sn')}  LONG ep_btc_ln {g('ep_btc_ln')}")

if __name__ == "__main__":
    main()
