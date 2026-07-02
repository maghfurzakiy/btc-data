#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
add_spot_cvd.py — tambah kolom SPOT-CVD + DIVERGENCE spot-perp + BASIS (backlog #4 + #6).

POST-PROCESSOR (pola sama add_volume_profile.py): baca CSV yang sudah ada, tarik Binance Vision
SPOT 1m-klines (+ REST spot utk hari berjalan), agregat ke per-jam, APPEND kolom di AKHIR skema
(urutan kolom lama utuh), atomic write (.tmp->replace), idempoten (overwrite kalau sudah ada).
Hanya pustaka standar.

SUMBER: data.binance.vision/data/spot/daily/klines/BTCUSDT/1m/...  +  api.binance.com REST (hari ini).
        LOKAL-ONLY (Binance geo-block 451 dari GitHub Actions). Taruh di run_local.bat, JANGAN cloud.

=== KOLOM BARU (#4 + #6) ===
  spot_close   : harga close SPOT per jam (referensi).
  basis        : close(PERP) - spot_close  [$].   (#6 sentimen leverage murni)
  basis_pct    : basis / spot_close * 100  [%].   + = perp premium (long-leverage), - = discount.
  cvd_spot     : CVD SPOT kumulatif (BTC), rumus cvd += 2*taker_buy - vol. (#4)
                 anchor = awal rentang yang ditarik (~awal CSV) — SLOPE yang penting, bukan level absolut.
  cvd_spot_d   : delta taker SPOT per-jam (BTC). + = beli agresif spot dominan jam itu.
  cvd_perp_d   : delta taker PERP per-jam (BTC) = diff kolom 'cvd' (perp) yang sudah ada.
  cvd_div      : cvd_perp_d - cvd_spot_d.   (#4 divergence)
                 + = perp lebih agresif beli dari spot (LEVERAGE-led, rapuh).
                 - = spot lebih agresif (REAL-demand-led, lebih sehat).

=== KENAPA #4 & #6 SATU FILE ===
  Dua-duanya butuh SATU sumber: Vision SPOT 1m. Sekali tarik -> spot_close (buat basis #6) DAN
  taker spot (buat CVD #4) langsung jadi, tanpa fetch tambahan.

=== BACKTEST QUESTIONS ===
  - basis_pct: gate sentimen — fade saat premium ekstrem (leverage over-long) / vice versa.
  - cvd_div: divergence S-signal — harga naik + cvd_div>0 + cvd_spot_d<0 = pump leverage = reversal-risk.

PEMAKAIAN
  python add_spot_cvd.py                          # default: btc_merged_hourly.csv di folder ini
  python add_spot_cvd.py btc_merged_hourly.csv    # path eksplisit
  python add_spot_cvd.py --selftest               # uji agregasi/CVD/basis tanpa jaringan
"""
import json, csv, os, io, sys, time, zipfile, urllib.request
from datetime import datetime, timezone, timedelta

SYMBOL    = "BTCUSDT"
BASE      = os.path.dirname(os.path.abspath(__file__))
CACHE     = os.path.join(BASE, "cache"); os.makedirs(CACHE, exist_ok=True)
SPOT_REST = "https://api.binance.com"
VISION_SPOT = "https://data.binance.vision/data/spot/daily/klines/%s/1m/%s-1m-%s.zip"
UA        = {"User-Agent": "Mozilla/5.0"}

NEW_COLS = ["spot_close", "basis", "basis_pct", "cvd_spot", "cvd_spot_d", "cvd_perp_d", "cvd_div"]

# ---------------- util jaringan (pola builder) ----------------
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

def _fetch(req, tries=3, backoff=1.5, timeout=60):
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

def _is_int(s):
    try: int(s); return True
    except: return False

def _to_sec(ts):
    """Normalisasi epoch ke DETIK apa pun satuannya (s/ms/us/ns).
       Binance Vision SPOT pakai mikrodetik (16 digit), futures pakai milidetik (13),
       REST pakai milidetik. Bagi 1000 sampai masuk rentang detik wajar."""
    ts = int(ts)
    while ts > 10_000_000_000:     # > ~thn 2286 dalam detik -> masih ms/us/ns
        ts //= 1000
    return ts

def parse_minute_csv(txt):
    """Vision/CSV 1m -> list (ts_sec, c, vol, tbv). open_time ms, taker_buy_base @ idx9."""
    out = []
    for row in csv.reader(io.StringIO(txt)):
        if not row or not _is_int(row[0]):
            continue
        try:
            ts = _to_sec(row[0])
            c, v, tbv = float(row[4]), float(row[5]), float(row[9])
            out.append((ts, c, v, tbv))
        except Exception:
            continue
    return out

def vision_day(ds):
    """spot 1m utk satu hari dari Vision ZIP, cache lokal. [] kalau gagal/belum ada."""
    cf = os.path.join(CACHE, f"spot1m-{ds}.csv")
    if os.path.exists(cf):
        return parse_minute_csv(open(cf, encoding="utf-8").read())
    try:
        raw = _fetch(urllib.request.Request(VISION_SPOT % (SYMBOL, SYMBOL, ds), headers=UA), tries=2)
        z = zipfile.ZipFile(io.BytesIO(raw)); txt = z.read(z.namelist()[0]).decode()
        open(cf, "w", encoding="utf-8").write(txt)
        return parse_minute_csv(txt)
    except Exception:
        return []

def rest_today():
    """hari berjalan (parsial) via REST spot 1m. [] kalau gagal."""
    start = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()*1000)
    out = []
    try:
        while True:
            arr = json.loads(_fetch(urllib.request.Request(
                f"{SPOT_REST}/api/v3/klines?symbol={SYMBOL}&interval=1m&startTime={start}&limit=1000",
                headers=UA)).decode())
            if not arr: break
            for k in arr:
                out.append((_to_sec(k[0]), float(k[4]), float(k[5]), float(k[9])))
            if len(arr) < 1000: break
            start = arr[-1][0] + 1; time.sleep(0.2)
        return out
    except Exception as e:
        print(f"    [warn] REST spot hari-ini gagal ({type(e).__name__}); pakai Vision saja.")
        return out

def floorH(ts): return ts - (ts % 3600)

# ---------------- agregasi per-jam ----------------
def hourly_spot(bars):
    """bars 1m spot -> {hour_ts: {'close':c,'delta':d,'vol':v}}. close = 1m terakhir di jam itu."""
    agg = {}
    for (ts, c, v, tbv) in sorted(bars, key=lambda x: x[0]):
        h = floorH(ts)
        e = agg.get(h)
        d = 2.0*tbv - v
        if e is None:
            agg[h] = {"close": c, "delta": d, "vol": v, "last_ts": ts}
        else:
            e["delta"] += d; e["vol"] += v
            if ts >= e["last_ts"]:
                e["close"] = c; e["last_ts"] = ts
    # CVD kumulatif spot (urut waktu)
    cum = 0.0
    for h in sorted(agg):
        cum += agg[h]["delta"]; agg[h]["cvd"] = round(cum, 2)
        agg[h]["delta"] = round(agg[h]["delta"], 2)
    return agg

# ---------------- main ----------------
def fnum(x):
    try:
        v = float(x); return v if v == v else None
    except (TypeError, ValueError):
        return None

def main(path):
    if not os.path.exists(path):
        print(f"[ERR] file tidak ada: {path}"); sys.exit(1)
    with open(path, encoding="utf-8", newline="") as f:
        rd = csv.reader(f); header = next(rd); rows = list(rd)
    idx = {c: i for i, c in enumerate(header)}
    for need in ("timestamp_utc", "close", "cvd"):
        if need not in idx:
            print(f"[ERR] kolom '{need}' tak ada — bukan CSV builder."); sys.exit(1)

    def epoch(s):
        try: return int(datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp())
        except: return None
    epochs = [epoch(r[idx["timestamp_utc"]]) for r in rows]
    valid = [e for e in epochs if e]
    if not valid:
        print("[ERR] tak ada timestamp valid."); sys.exit(1)
    d_start = datetime.fromtimestamp(min(valid), timezone.utc).date()
    d_today = datetime.now(timezone.utc).date()

    print(f"[{datetime.now(timezone.utc).isoformat()}] add_spot_cvd  spot 1m: {d_start} .. {d_today}  (cache: {CACHE})")
    bars = []; day = d_start; n = 0
    while day < d_today:
        bars += vision_day(day.strftime("%Y-%m-%d")); n += 1
        if n % 30 == 0: print(f"    ...{day} ({len(bars)} bar)")
        day += timedelta(days=1)
    bars += rest_today()
    if not bars:
        print("[ERR] 0 bar spot 1m (jaringan/geo-block?). CSV TIDAK diubah."); sys.exit(1)
    agg = hourly_spot(bars)
    print(f"  spot 1m bar: {len(bars)}  | jam ter-agregat: {len(agg)}")

    header_out = header + [c for c in NEW_COLS if c not in idx]
    out_idx = {c: i for i, c in enumerate(header_out)}

    fill = {c: 0 for c in NEW_COLS}
    prev_perp_cvd = None
    out_rows = []
    for r, T in zip(rows, epochs):
        r = list(r) + [""] * (len(header_out) - len(r))
        perp_close = fnum(r[idx["close"]])
        perp_cvd = fnum(r[idx["cvd"]])

        sc = bp = bpct = cvds = cvdsd = ""
        if T is not None:
            h = floorH(T); e = agg.get(h)
            if e:
                sc = round(e["close"], 1)
                cvds = e["cvd"]; cvdsd = e["delta"]
                if perp_close is not None and sc:
                    bp = round(perp_close - sc, 1)
                    bpct = round((perp_close - sc) / sc * 100.0, 4)

        # perp hourly delta (diff cvd) + divergence
        cpd = ""
        if perp_cvd is not None and prev_perp_cvd is not None:
            cpd = round(perp_cvd - prev_perp_cvd, 2)
        if perp_cvd is not None:
            prev_perp_cvd = perp_cvd
        cdv = round(cpd - cvdsd, 2) if (cpd != "" and cvdsd != "") else ""

        for c, val in zip(NEW_COLS, [sc, bp, bpct, cvds, cvdsd, cpd, cdv]):
            r[out_idx[c]] = val
            if val != "":
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
        print(f"     {c:11s} terisi {fill[c]}/{len(out_rows)}")
    if out_rows:
        last = out_rows[-1]; g = lambda c: last[out_idx[c]]
        print(f"     baris akhir: spot {g('spot_close')}  basis {g('basis')} ({g('basis_pct')}%)  "
              f"cvd_spot {g('cvd_spot')}  spotΔ {g('cvd_spot_d')}  perpΔ {g('cvd_perp_d')}  div {g('cvd_div')}")

# ---------------- selftest (tanpa jaringan) ----------------
def selftest():
    print("=== SELFTEST add_spot_cvd ===")
    # 3 jam sintetis, 2 menit/jam. taker_buy & vol direka.
    H = 3600
    bars = [
        # (ts, close, vol, tbv)
        (0*H+60,  60000, 100, 70),   # jam0: delta = 2*70-100 = +40 ; +  (lalu)
        (0*H+120, 60050, 100, 60),   #        delta = +20  -> jam0 total +60, close 60050
        (1*H+60,  60100,  80, 30),   # jam1: delta = 2*30-80 = -20
        (1*H+120, 60020,  80, 20),   #        delta = -40 -> jam1 total -60, close 60020
        (2*H+60,  60080, 120, 90),   # jam2: delta = +60
        (2*H+120, 60120, 120, 95),   #        delta = +70 -> jam2 total +130, close 60120
    ]
    agg = hourly_spot(bars)
    ks = sorted(agg)                       # key = floorH(ts) -> {0, 3600, 7200}
    for h in ks:
        e = agg[h]; print(f" jam {h//H}: close {e['close']} delta {e['delta']} cvd {e['cvd']}")
    a0, a1, a2 = agg[ks[0]], agg[ks[1]], agg[ks[2]]
    ok = (a0["delta"] == 60 and a1["delta"] == -60 and a2["delta"] == 130
          and a2["cvd"] == 130 and a0["close"] == 60050 and a2["close"] == 60120)
    # basis + divergence contoh
    perp_close, spot_close = 60200.0, 60120.0
    basis = round(perp_close - spot_close, 1); bpct = round(basis/spot_close*100, 4)
    perp_d, spot_d = 200.0, 130.0; div = round(perp_d - spot_d, 2)
    print(f" basis={basis} ({bpct}%)  div(perpΔ-spotΔ)={div} (harap +70 = leverage-led)")
    print(" RESULT:", "OK ✅" if (ok and basis == 80.0 and div == 70.0) else "CEK ❌")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest(); sys.exit(0)
    csv_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(BASE, "btc_merged_hourly.csv")
    try:
        main(csv_path)
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"\n[FATAL] {type(e).__name__}: {e}\n[INFO] CSV lama TIDAK diubah (atomic .tmp).")
        sys.exit(1)
