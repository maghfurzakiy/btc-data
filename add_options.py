#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
add_options.py — tambah kolom OPTIONS/GEX (opt_) ke btc_merged_hourly.csv.

POST-PROCESSOR (pola sama add_volume_profile.py / add_spot_cvd.py): baca CSV yang sudah ada,
tarik OPTION CHAIN Deribit (SATU call publik, tanpa API key), HITUNG SENDIRI GEX / max-pain /
PCR / gamma-flip / walls / ATM-IV, lalu APPEND kolom di AKHIR skema (urutan kolom lama utuh),
atomic write (.tmp->replace), idempoten (overwrite kalau sudah ada). HANYA pustaka standar.

=== KENAPA DERIBIT (bukan scrape Laevitas/CryptoGamma) ===
  GEX/max-pain/PCR/gamma-flip itu METRIK TURUNAN dari SATU sumber mentah: option-chain.
  Laevitas & CryptoGamma tinggal re-compute dari Deribit (~85-90% OI options BTC = Deribit).
  Jadi kita hitung sendiri dari sumber -> $0, tanpa key, transparan (sign-convention di tangan kita),
  auditable, reproducible. 1-2 network call. Ini GANTIIN semua screenshot options.

=== SUMBER ===
  GET https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option
      -> seluruh chain: instrument_name, open_interest, mark_iv, underlying_price, mark_price, volume.
  (opsional) get_index_price?index_name=btc_usd utk spot referensi.
  Gamma DIHITUNG Black-Scholes dari mark_iv (Deribit tak kirim gamma di book_summary).

=== SEMANTIK: SNAPSHOT, bukan historis ===
  Deribit publik = snapshot SEKARANG (tak ada historical GEX gratis). Supaya jadi kolom per-jam
  yang AKUMULATIF ke depan & tahan regenerasi build: tiap run di-stamp ke cache/opt_snapshot.csv
  (append per-jam, last-write-wins — pola upnl_history.csv), lalu JOIN by floor-hour ke merged CSV.
  Baris sebelum run pertama = KOSONG (jujur; historis butuh sumber berbayar Laevitas/Amberdata).

=== KOLOM BARU (prefix opt_) ===
  opt_spot        : underlying/index BTC saat snapshot ($).
  opt_gex_total   : NET dealer gamma exposure, $M per 1% move (bertanda).
                    + = dealer NET-LONG gamma (pinning/mean-revert, range).  - = NET-SHORT (amplify tren).
  opt_gex_flip    : harga saat cumulative net-GEX lintas 0 (di bawah = neg-gamma accel). [aproksimasi by-strike]
  opt_pin         : strike +gamma terbesar dekat spot (magnet pin).
  opt_call_wall   : strike +GEX terbesar DI ATAS spot (resist).
  opt_put_wall    : strike -GEX terbesar DI BAWAH spot (support).
  opt_max_pain    : max-pain expiry TERDEKAT (harga minimal payout ke holder).
  opt_pcr_oi      : Put/Call ratio by OI (semua expiry). >1 = put-heavy.
  opt_iv_atm      : ATM implied vol % (expiry terdekat, strike terdekat spot).
  opt_oi_total    : total OI options (BTC, semua expiry).

=== MODELING CHOICE (satu-satunya; sadar & bisa di-flip) ===
  DEALER_LONG_CALLS = True  -> net GEX = callGEX - putGEX (dealer long call-gamma, short put-gamma).
  Ganti ke False kalau mau konvensi kebalikan. SIGN & level relatif yang penting, bukan angka absolut.

=== BACKTEST QUESTIONS ===
  - opt_gex_total<0 (neg-gamma) = regime amplify -> fade lebih berisiko, momentum jalan?
  - harga < opt_gex_flip = akselerasi turun (validasi vs realized move fwd-6/24h)?
  - opt_pin sebagai magnet: |close-pin| mengecil menjelang expiry?
  - opt_pcr_oi & opt_iv_atm sebagai filter sentimen.

PEMAKAIAN
  python add_options.py                          # default: btc_merged_hourly.csv di folder ini
  python add_options.py btc_merged_hourly.csv    # path eksplisit
  python add_options.py --selftest               # uji BS-gamma/max-pain/PCR tanpa jaringan
  python add_options.py --dry                     # fetch+print snapshot, TIDAK tulis CSV
  python add_options.py --greeks                  # pakai gamma ASLI ticker Deribit (akurat > BS; N call)
      (auto-aktif kalau mark_iv coverage < 50% di book_summary)

Taruh di run_local.bat SETELAH build (langkah baru), NOL ketergantungan ke langkah lain.
"""
import json, csv, os, io, sys, time, math, urllib.request
from datetime import datetime, timezone

SYMBOL   = "BTC"
BASE     = os.path.dirname(os.path.abspath(__file__))
CACHE    = os.path.join(BASE, "cache"); os.makedirs(CACHE, exist_ok=True)
SNAP     = os.path.join(CACHE, "opt_snapshot.csv")
DERIBIT  = "https://www.deribit.com/api/v2/public"
UA       = {"User-Agent": "Mozilla/5.0"}

DEALER_LONG_CALLS = True    # net GEX = callGEX - putGEX (lihat MODELING CHOICE)
RISK_FREE         = 0.0     # r utk BS (BTC ~ pakai 0; efek kecil pada gamma)
MIN_T_YEARS       = 1/8760  # floor 1 jam (hindari gamma meledak di 0DTE)
YEAR_SEC          = 365.0 * 24 * 3600

NEW_COLS = ["opt_spot", "opt_gex_total", "opt_gex_flip", "opt_pin", "opt_call_wall",
            "opt_put_wall", "opt_max_pain", "opt_pcr_oi", "opt_iv_atm", "opt_oi_total"]

MONTHS = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
          "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}

# ---------------- util jaringan (TLS-cascade, pola engine kamu) ----------------
import ssl, urllib.error
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
def _fetch(req, tries=3, backoff=1.5, timeout=60):
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
                _CTX_WINNER = i; raise
            except Exception as e:
                last = e
        print(f"    retry {attempt+1}/{tries} (semua ctx gagal: {type(last).__name__}) ...")
        time.sleep(backoff*(attempt+1))
    raise last

def fnum(x):
    try:
        v = float(x); return v if v == v else None
    except (TypeError, ValueError):
        return None

# ---------------- Deribit ----------------
def deribit_chain():
    """Ambil seluruh option-chain BTC (1 call). -> list dict + spot referensi."""
    url = f"{DERIBIT}/get_book_summary_by_currency?currency={SYMBOL}&kind=option"
    raw = _fetch(urllib.request.Request(url, headers=UA))
    data = json.loads(raw.decode())
    if "result" not in data:
        raise RuntimeError(f"respons Deribit tak terduga: {str(data)[:200]}")
    return data["result"]

def deribit_index():
    try:
        raw = _fetch(urllib.request.Request(f"{DERIBIT}/get_index_price?index_name=btc_usd", headers=UA))
        return fnum(json.loads(raw.decode())["result"]["index_price"])
    except Exception:
        return None

def deribit_ticker(instrument):
    """Ticker 1 instrumen: gamma ASLI (greeks.gamma) + mark_iv. Dipakai fallback kalau
       book_summary tak kirim mark_iv, atau saat --greeks (akurasi > BS)."""
    raw = _fetch(urllib.request.Request(f"{DERIBIT}/ticker?instrument_name={instrument}", headers=UA))
    r = json.loads(raw.decode()).get("result", {})
    gk = r.get("greeks") or {}
    return {"gamma": fnum(gk.get("gamma")), "iv": fnum(r.get("mark_iv")),
            "u": fnum(r.get("underlying_price")), "oi": fnum(r.get("open_interest"))}

# ---------------- Black-Scholes gamma ----------------
def _norm_pdf(x): return math.exp(-0.5*x*x) / math.sqrt(2.0*math.pi)
def bs_gamma(S, K, T, sigma, r=0.0):
    if not (S > 0 and K > 0 and T > 0 and sigma > 0):
        return 0.0
    d1 = (math.log(S/K) + (r + 0.5*sigma*sigma)*T) / (sigma*math.sqrt(T))
    return _norm_pdf(d1) / (S * sigma * math.sqrt(T))

# ---------------- parsing instrumen ----------------
def parse_instrument(name):
    """'BTC-2JUL26-60000-C' -> (expiry_ts_sec, strike, 'C'|'P') atau None."""
    try:
        _, ds, strike, cp = name.split("-")
        cp = cp.upper()
        if cp not in ("C", "P"): return None
        mon = MONTHS[ds[-5:-2]]
        day = int(ds[:-5]); yy = 2000 + int(ds[-2:])
        exp = datetime(yy, mon, day, 8, 0, 0, tzinfo=timezone.utc)  # Deribit expiry 08:00 UTC
        return (int(exp.timestamp()), float(strike), cp)
    except Exception:
        return None

# ---------------- compute snapshot ----------------
def compute_snapshot(chain, now_ts, spot_hint=None, greeks_map=None):
    """chain = list dict Deribit book_summary. greeks_map = {instrument: {gamma,iv}} (opsional,
       gamma asli dari ticker). -> dict metrik opt_."""
    greeks_map = greeks_map or {}
    opts = []
    und = []
    for it in chain:
        name = it.get("instrument_name", "")
        p = parse_instrument(name)
        if not p: continue
        exp, K, cp = p
        oi = fnum(it.get("open_interest"))
        u  = fnum(it.get("underlying_price"))
        if u: und.append(u)
        if oi is None or oi <= 0: continue
        gm = greeks_map.get(name) or {}
        iv = gm.get("iv") if gm.get("iv") is not None else fnum(it.get("mark_iv"))   # persen
        o = {"exp": exp, "K": K, "cp": cp, "oi": oi,
             "iv": (iv/100.0 if iv is not None else None), "u": u,
             "vol": fnum(it.get("volume"))}
        if gm.get("gamma") is not None:
            o["gamma"] = gm["gamma"]
        opts.append(o)
    if not opts:
        return None
    iv_cov = round(sum(1 for o in opts if o.get("gamma") is not None or o["iv"] is not None)/len(opts)*100, 0)
    S = spot_hint or (sorted(und)[len(und)//2] if und else None)
    if S is None:
        return None

    # --- GEX per instrumen + agregasi per-strike ---
    gex_strike = {}   # strike -> net GEX ($/1%)
    net_gex = 0.0
    for o in opts:
        g = o.get("gamma")                                    # gamma ASLI (ticker) kalau ada
        if g is None:                                         # else Black-Scholes dari mark_iv
            T = max((o["exp"] - now_ts) / YEAR_SEC, MIN_T_YEARS)
            g = bs_gamma(S, o["K"], T, o["iv"], RISK_FREE) if o["iv"] else 0.0
        sign = 1.0 if o["cp"] == "C" else -1.0
        if not DEALER_LONG_CALLS: sign = -sign
        gex = g * o["oi"] * S * S * 0.01 * sign      # $ per 1% move
        net_gex += gex
        gex_strike[o["K"]] = gex_strike.get(o["K"], 0.0) + gex

    strikes = sorted(gex_strike)
    # gamma flip: cumulative net-GEX (by strike asc) lintas 0 -> aproksimasi harga flip
    flip = None
    cum = 0.0; prev_k = None; prev_cum = 0.0
    for k in strikes:
        cum += gex_strike[k]
        if prev_k is not None and (prev_cum < 0 <= cum or prev_cum > 0 >= cum):
            # interpolasi linear titik silang
            span = cum - prev_cum
            flip = prev_k + (k - prev_k) * ((0 - prev_cum)/span if span else 0.5)
            break
        prev_k, prev_cum = k, cum
    # pin: strike |+GEX| terbesar dekat spot (±10%)
    near = [k for k in strikes if abs(k - S)/S <= 0.10]
    pin = max(near, key=lambda k: gex_strike[k]) if near else None
    # walls
    above = [k for k in strikes if k > S]
    below = [k for k in strikes if k < S]
    call_wall = max(above, key=lambda k: gex_strike[k]) if above else None      # +GEX terbesar di atas
    put_wall  = min(below, key=lambda k: gex_strike[k]) if below else None      # -GEX terbesar (min) di bawah

    # --- PCR OI + total OI ---
    call_oi = sum(o["oi"] for o in opts if o["cp"] == "C")
    put_oi  = sum(o["oi"] for o in opts if o["cp"] == "P")
    pcr = round(put_oi/call_oi, 3) if call_oi > 0 else ""
    oi_total = round(call_oi + put_oi, 1)

    # --- max pain expiry TERDEKAT (>= now, OI cukup) ---
    exps = sorted(set(o["exp"] for o in opts if o["exp"] > now_ts))
    max_pain = ""
    for e in exps:
        grp = [o for o in opts if o["exp"] == e]
        if sum(o["oi"] for o in grp) < 50:   # skip expiry OI tipis
            continue
        ks = sorted(set(o["K"] for o in grp))
        def payout(P):
            return sum(o["oi"]*max(0.0, (P-o["K"]) if o["cp"]=="C" else (o["K"]-P)) for o in grp)
        max_pain = min(ks, key=payout)
        break

    # --- ATM IV (expiry terdekat, strike terdekat spot) ---
    iv_atm = ""
    if exps:
        e0 = exps[0]
        cand = [o for o in opts if o["exp"] == e0 and o["iv"]]
        if cand:
            a = min(cand, key=lambda o: abs(o["K"] - S))
            iv_atm = round(a["iv"]*100.0, 1)

    return {
        "opt_spot":      round(S, 1),
        "opt_gex_total": round(net_gex/1e6, 2),   # $M / 1%
        "opt_gex_flip":  ("" if flip is None else round(flip, 0)),
        "opt_pin":       ("" if pin is None else round(pin, 0)),
        "opt_call_wall": ("" if call_wall is None else round(call_wall, 0)),
        "opt_put_wall":  ("" if put_wall is None else round(put_wall, 0)),
        "opt_max_pain":  ("" if max_pain == "" else round(max_pain, 0)),
        "opt_pcr_oi":    pcr,
        "opt_iv_atm":    iv_atm,
        "opt_oi_total":  oi_total,
        "_iv_cov":       iv_cov,   # diagnostik, TIDAK ditulis (bukan di NEW_COLS)
    }

# ---------------- cache snapshot (append per-jam, last-write-wins) ----------------
def floorH(ts): return ts - (ts % 3600)

def load_snap_cache():
    rows = {}
    if os.path.exists(SNAP):
        with open(SNAP, encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                h = int(r["hour_ts"])
                rows[h] = {k: r.get(k, "") for k in NEW_COLS}
    return rows

def save_snap_cache(cache):
    tmp = SNAP + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["hour_ts"] + NEW_COLS)
        for h in sorted(cache):
            w.writerow([h] + [cache[h].get(c, "") for c in NEW_COLS])
    os.replace(tmp, SNAP)

# ---------------- main ----------------
def epoch(s):
    try: return int(datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp())
    except Exception: return None

def main(path, dry=False, use_greeks=False):
    print(f"[{datetime.now(timezone.utc).isoformat()}] add_options  (Deribit chain -> GEX/max-pain/PCR)")
    chain = deribit_chain()
    now_ts = int(datetime.now(timezone.utc).timestamp())
    spot = deribit_index()
    if spot is None:
        u = [fnum(it.get("underlying_price")) for it in chain if fnum(it.get("underlying_price"))]
        spot = sorted(u)[len(u)//2] if u else None

    # coverage IV dari book_summary; kalau rendah / --greeks -> tarik gamma asli per-instrumen (near-money, OI>0)
    have_iv = sum(1 for it in chain if fnum(it.get("mark_iv")) is not None
                  and (fnum(it.get("open_interest")) or 0) > 0 and parse_instrument(it.get("instrument_name","")))
    total_oi_inst = sum(1 for it in chain if (fnum(it.get("open_interest")) or 0) > 0 and parse_instrument(it.get("instrument_name","")))
    cov0 = (have_iv/total_oi_inst*100) if total_oi_inst else 0
    greeks_map = {}
    if (use_greeks or cov0 < 50) and spot:
        near = [it for it in chain
                if (fnum(it.get("open_interest")) or 0) > 0 and parse_instrument(it.get("instrument_name",""))
                and abs(parse_instrument(it["instrument_name"])[1]-spot)/spot <= 0.25]
        why = "flag --greeks" if use_greeks else f"mark_iv coverage rendah ({cov0:.0f}%)"
        print(f"  [greeks] {why} -> tarik ticker gamma-asli utk {len(near)} instrumen near-money (±25%) ...")
        for k, it in enumerate(near):
            nm = it["instrument_name"]
            try:
                greeks_map[nm] = deribit_ticker(nm)
            except Exception as e:
                print(f"    warn ticker {nm}: {type(e).__name__}")
            if k and k % 40 == 0: time.sleep(0.3)   # santun ke rate-limit (~20 req/s)

    snap = compute_snapshot(chain, now_ts, spot_hint=spot, greeks_map=greeks_map)
    if snap is None:
        print("[ERR] chain kosong / tak bisa hitung snapshot. CSV TIDAK diubah."); sys.exit(1)
    print(f"  instrumen chain: {len(chain)}  | IV/gamma coverage: {snap['_iv_cov']:.0f}%  | snapshot:")
    for k in NEW_COLS: print(f"     {k:14s}: {snap[k]}")
    gx = snap["opt_gex_total"]
    print(f"  >>> REGIME GAMMA: {'NET-LONG (+, pinning/range)' if isinstance(gx,(int,float)) and gx>=0 else 'NET-SHORT (-, amplify tren)'}")

    if dry:
        print("  [dry] tidak menulis CSV / cache."); return

    # stamp ke cache per-jam (last-write-wins)
    cache = load_snap_cache()
    cache[floorH(now_ts)] = {k: snap[k] for k in NEW_COLS}
    save_snap_cache(cache)
    print(f"  cache: {SNAP} ({len(cache)} jam)")

    if not os.path.exists(path):
        print(f"[ERR] file merged tak ada: {path}"); sys.exit(1)
    with open(path, encoding="utf-8", newline="") as f:
        rd = csv.reader(f); header = next(rd); rows = list(rd)
    idx = {c: i for i, c in enumerate(header)}
    if "timestamp_utc" not in idx:
        print("[ERR] kolom timestamp_utc tak ada — bukan CSV builder."); sys.exit(1)

    header_out = header + [c for c in NEW_COLS if c not in idx]
    out_idx = {c: i for i, c in enumerate(header_out)}
    fill = {c: 0 for c in NEW_COLS}
    out_rows = []
    for r in rows:
        r = list(r) + [""]*(len(header_out)-len(r))
        T = epoch(r[idx["timestamp_utc"]])
        rec = cache.get(floorH(T)) if T is not None else None
        for c in NEW_COLS:
            v = rec.get(c, "") if rec else ""
            r[out_idx[c]] = v
            if v not in ("", None): fill[c] += 1
        out_rows.append(r)

    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(header_out); w.writerows(out_rows)
    os.replace(tmp, path)

    appended = [c for c in NEW_COLS if c not in idx]
    print(f"[OK] {path}")
    print(f"     kolom: {'+'.join(appended) if appended else '(sudah ada, overwrite)'}  | baris: {len(out_rows)}")
    for c in NEW_COLS:
        print(f"     {c:14s} terisi {fill[c]}/{len(out_rows)}")
    print("     (kolom snapshot: terisi hanya jam yang pernah di-run — akumulatif ke depan)")

# ---------------- selftest (tanpa jaringan) ----------------
def selftest():
    print("=== SELFTEST add_options ===")
    # BS gamma: ATM > OTM, dan simetris kira2
    S = 60000.0
    g_atm = bs_gamma(S, 60000, 30/365, 0.5)
    g_otm = bs_gamma(S, 70000, 30/365, 0.5)
    print(f" gamma ATM {g_atm:.3e} > OTM {g_otm:.3e} : {'OK' if g_atm > g_otm else 'CEK'}")

    # chain sintetis (mirip book_summary): 1 expiry ~7 hari
    now = int(datetime.now(timezone.utc).timestamp())
    exp = now + 7*24*3600
    ds = datetime.fromtimestamp(exp, timezone.utc)
    tag = f"{ds.day}{['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'][ds.month-1]}{str(ds.year)[2:]}"
    chain = []
    def add(K, cp, oi, iv):
        chain.append({"instrument_name": f"BTC-{tag}-{K}-{cp}", "open_interest": oi,
                      "mark_iv": iv, "underlying_price": 60000.0, "volume": 10})
    # calls di atas, puts di bawah + konsentrasi call OI di 62000, put OI di 58000
    add(58000,"P",800,45); add(59000,"P",400,42); add(60000,"C",300,40); add(60000,"P",300,40)
    add(61000,"C",500,41); add(62000,"C",1200,43); add(64000,"C",200,46)
    snap = compute_snapshot(chain, now, spot_hint=60000.0)
    for k in NEW_COLS: print(f"   {k:14s}: {snap[k]}")
    call_oi = 300+500+1200+200; put_oi = 800+400+300
    pcr_ok = abs(snap["opt_pcr_oi"] - round(put_oi/call_oi,3)) < 1e-6
    # max pain harus di rentang strike (bukan kosong), pin dekat spot
    mp_ok  = snap["opt_max_pain"] != "" and 58000 <= snap["opt_max_pain"] <= 64000
    oi_ok  = abs(snap["opt_oi_total"] - (call_oi+put_oi)) < 1e-6
    cw_ok  = snap["opt_call_wall"] in (61000,62000,64000)   # wall call di atas spot
    pw_ok  = snap["opt_put_wall"] in (58000,59000)          # wall put di bawah spot
    print(" PCR", "OK" if pcr_ok else "CEK", "| max_pain", "OK" if mp_ok else "CEK",
          "| OI", "OK" if oi_ok else "CEK", "| call_wall", "OK" if cw_ok else "CEK",
          "| put_wall", "OK" if pw_ok else "CEK")
    print(" RESULT:", "OK ✅" if (g_atm>g_otm and pcr_ok and mp_ok and oi_ok and cw_ok and pw_ok) else "CEK ❌")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest(); sys.exit(0)
    dry = "--dry" in sys.argv
    use_greeks = "--greeks" in sys.argv
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    csv_path = args[0] if args else os.path.join(BASE, "btc_merged_hourly.csv")
    try:
        main(csv_path, dry=dry, use_greeks=use_greeks)
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"\n[FATAL] {type(e).__name__}: {e}\n[INFO] CSV lama TIDAK diubah (atomic .tmp).")
        sys.exit(1)
