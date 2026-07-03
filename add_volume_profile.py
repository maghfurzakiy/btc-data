#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
add_volume_profile.py — tambah kolom AMT (Volume Profile) ke btc_merged_hourly.csv.

POST-PROCESSOR (pola sama add_long_notional.py): baca CSV yang sudah ada, hitung
profil volume ROLLING per baris (window mundur N jam, no-lookahead), APPEND kolom di
AKHIR skema (urutan kolom lama utuh), atomic write, idempoten (overwrite kalau sudah ada).

SUMBER DATA: Binance Vision daily 1m-klines ZIP (pola identik get_metrics di builder) +
REST fapi utk hari berjalan (parsial). 1m-kline punya taker_buy_volume -> bisa hitung
DELTA (proxy footprint) tanpa aggTrades. Untuk delta-at-price SEJATI (per-trade) nanti
bisa di-upgrade ke aggTrades; 1m sudah cukup utk POC/VAH/VAL + delta agregat.

KOLOM BARU (default prefix 'vp_', window 24h):
  vp_poc        : Volume Point of Control (harga bin volume terbesar)         [Volume-POC, = garis MERAH TV]
  vp_vah        : Value Area High  (batas atas 70% volume)
  vp_val        : Value Area Low   (batas bawah 70% volume)
  vp_delta      : net taker delta SELURUH window (BTC; + = beli agresif dominan)
  vp_poc_delta  : net taker delta DI bin POC (absorpsi/initiative di POC)      [sinyal ortogonal S5]
  vp_pos        : posisi close vs value-area: above_vah | in_va | below_val
  vp_poc_dist   : (close - POC)/close * 100  (% jarak ke POC, + = di atas POC)  [fitur magnet S2]

PEMAKAIAN
  python add_volume_profile.py                         # default: btc_merged_hourly.csv, window 24h, bin $25, prefix vp_
  python add_volume_profile.py btc_merged_hourly.csv   # path eksplisit
  python add_volume_profile.py btc_merged_hourly.csv 96 25 vp96_   # window 96h, bin 25, prefix vp96_ (profil swing)
  python add_volume_profile.py --selftest              # uji logika POC/VA tanpa jaringan

CATATAN PENTING
  - LOKAL-ONLY: Binance kena geo-block (451) dari GitHub Actions. JANGAN taruh di cloud (merged.yml).
    Jalankan di run_local.bat SETELAH build_merged_dataset.py.
  - Multi-TF: jalankan beberapa kali dgn window+prefix beda (mis. vp_ 24h, vp96_ 96h) -> kolom terpisah.
  - cache 1m gede: file cache klines1m-*.csv sebaiknya di-_gitignore (re-download dari Vision).
  Hanya pustaka standar Python.
"""
import json, csv, os, io, sys, time, zipfile, urllib.request
from datetime import datetime, timezone, timedelta

# ---------------- KONFIG ----------------
SYMBOL   = "BTCUSDT"
WINDOW_H = 24          # lookback value-area (jam). Ganti via argv[2]. 6=intraday, 24=harian, 96=swing.
BIN_USD  = 25.0        # lebar bin harga ($). Ganti via argv[3].
PREFIX   = "vp_"       # prefix kolom. Ganti via argv[4] utk multi-TF.
VA_FRAC  = 0.70        # fraksi value-area (standar 70%)
BASE     = os.path.dirname(os.path.abspath(__file__))
CACHE    = os.path.join(BASE, "cache"); os.makedirs(CACHE, exist_ok=True)
FAPI     = "https://fapi.binance.com"
VISION_KL= "https://data.binance.vision/data/futures/um/daily/klines/%s/1m/%s-1m-%s.zip"
UA       = {"User-Agent": "Mozilla/5.0"}

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
       Binance Vision SPOT pakai mikrodetik (16 digit), futures pakai milidetik (13).
       Bagi 1000 sampai masuk rentang detik wajar -> kebal kalau futures pindah µs."""
    ts = int(ts)
    while ts > 10_000_000_000:
        ts //= 1000
    return ts

def parse_minute_csv(txt):
    """Vision/CSV 1m -> list (ts_sec, o,h,l,c,vol,tbv). Tahan header opsional."""
    out = []
    rd = csv.reader(io.StringIO(txt))
    for row in rd:
        if not row or not _is_int(row[0]):   # skip header / baris rusak
            continue
        try:
            ts = _to_sec(row[0])              # open_time ms/us -> s (kebal perubahan satuan)
            o,h,l,c,v = float(row[1]),float(row[2]),float(row[3]),float(row[4]),float(row[5])
            tbv = float(row[9])               # taker_buy_volume (base)
            out.append((ts,o,h,l,c,v,tbv))
        except Exception:
            continue
    return out

def vision_day(ds):
    """1m bars utk satu hari (YYYY-MM-DD) dari Vision ZIP, cache lokal. [] kalau gagal."""
    cf = os.path.join(CACHE, f"klines1m-{ds}.csv")
    if os.path.exists(cf):
        return parse_minute_csv(open(cf, encoding="utf-8").read())
    try:
        raw = _fetch(urllib.request.Request(VISION_KL % (SYMBOL, SYMBOL, ds), headers=UA), tries=2)
        z = zipfile.ZipFile(io.BytesIO(raw)); txt = z.read(z.namelist()[0]).decode()
        open(cf, "w", encoding="utf-8").write(txt)
        return parse_minute_csv(txt)
    except Exception:
        return []   # file belum ada (hari ini) / gagal -> ditangani pemanggil

def rest_today():
    """Hari berjalan (parsial) via REST fapi 1m. [] kalau gagal."""
    start = int(datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0).timestamp()*1000)
    try:
        arr = json.loads(_fetch(urllib.request.Request(
            f"{FAPI}/fapi/v1/klines?symbol={SYMBOL}&interval=1m&startTime={start}&limit=1500", headers=UA)).decode())
        return [(_to_sec(k[0]), float(k[1]),float(k[2]),float(k[3]),float(k[4]),float(k[5]),float(k[9])) for k in arr]
    except Exception as e:
        print(f"    [warn] REST hari-ini gagal ({type(e).__name__}); pakai Vision saja."); return []

# ---------------- profil ----------------
def bin_of(price): return int(round(price / BIN_USD))

def precompute_minutes(bars):
    """bar 1m -> (ts, bin, vol, delta). delta=2*tbv-vol (BTC). Pakai typical-price (H+L+C)/3."""
    out = []
    for (ts,o,h,l,c,v,tbv) in bars:
        tp = (h + l + c) / 3.0
        out.append((ts, bin_of(tp), v, 2.0*tbv - v))
    out.sort(key=lambda x: x[0])
    return out

def profile_fields(hist, close):
    """hist: {bin:[vol,delta]} -> (poc,vah,val,total_delta,poc_delta,pos,dist)."""
    if not hist: return ("","","","","","","")
    bins = sorted(hist.keys())
    vols = [hist[b][0] for b in bins]
    total = sum(vols)
    if total <= 0: return ("","","","","","","")
    # POC
    pidx = max(range(len(bins)), key=lambda i: vols[i])
    poc_bin = bins[pidx]
    poc_price = poc_bin * BIN_USD
    # Value Area 70% (klasik: ekspansi dari POC, ambil sisi lebih besar)
    target = VA_FRAC * total
    acc = vols[pidx]; lo = hi = pidx
    while acc < target and (lo > 0 or hi < len(bins)-1):
        up = vols[hi+1] if hi < len(bins)-1 else -1
        dn = vols[lo-1] if lo > 0 else -1
        if up >= dn:
            hi += 1; acc += max(up,0)
        else:
            lo -= 1; acc += max(dn,0)
    vah = bins[hi] * BIN_USD
    val = bins[lo] * BIN_USD
    total_delta = sum(hist[b][1] for b in bins)
    poc_delta = hist[poc_bin][1]
    # posisi close vs VA
    if close is None: pos = ""
    elif close > vah: pos = "above_vah"
    elif close < val: pos = "below_val"
    else:             pos = "in_va"
    dist = "" if close in (None,0) else round((close - poc_price)/close*100.0, 3)
    return (round(poc_price,1), round(vah,1), round(val,1),
            round(total_delta,1), round(poc_delta,1), pos, dist)

# ---------------- main ----------------
def main(csv_path):
    if not os.path.exists(csv_path):
        print(f"[ERR] file tidak ada: {csv_path}"); sys.exit(1)

    with open(csv_path, encoding="utf-8", newline="") as f:
        rd = csv.reader(f); header = next(rd); rows = list(rd)
    idx = {c:i for i,c in enumerate(header)}
    if "timestamp_utc" not in idx or "close" not in idx:
        print("[ERR] kolom timestamp_utc/close tak ada — bukan CSV builder."); sys.exit(1)

    new_cols = [PREFIX+s for s in ("poc","vah","val","delta","poc_delta","pos","poc_dist")]
    header_out = header + [c for c in new_cols if c not in idx]
    out_idx = {c:i for i,c in enumerate(header_out)}

    # epoch tiap baris
    def epoch(s):
        try: return int(datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp())
        except: return None
    epochs = [epoch(r[idx["timestamp_utc"]]) for r in rows]
    valid  = [e for e in epochs if e]
    if not valid: print("[ERR] tak ada timestamp valid."); sys.exit(1)
    t_min, t_max = min(valid), max(valid)

    # rentang hari yang perlu di-fetch (mundur window utk baris paling awal)
    d_start = datetime.fromtimestamp(t_min, timezone.utc).date() - timedelta(days=(WINDOW_H//24)+1)
    d_today = datetime.now(timezone.utc).date()

    print(f"[{datetime.now(timezone.utc).isoformat()}] add_volume_profile  window={WINDOW_H}h bin=${BIN_USD:.0f} prefix='{PREFIX}'")
    print(f"  fetch 1m: {d_start} .. {d_today}  (cache: {CACHE})")
    bars = []
    day = d_start; ndl = 0
    while day < d_today:
        ds = day.strftime("%Y-%m-%d")
        b = vision_day(ds)
        bars += b; ndl += 1
        if ndl % 20 == 0: print(f"    ...{ds} ({len(bars)} bar)")
        day += timedelta(days=1)
    bars += rest_today()   # hari ini parsial via REST
    if not bars:
        print("[ERR] 0 bar 1m terkumpul (jaringan?). CSV tidak diubah."); sys.exit(1)
    mins = precompute_minutes(bars)
    print(f"  total 1m bar: {len(mins)}")

    # sliding window add/remove (no-lookahead): window = (T-WINDOW, T]
    W = WINDOW_H * 3600
    hist = {}
    def add(b,v,d):
        e = hist.get(b)
        if e: e[0]+=v; e[1]+=d
        else: hist[b]=[v,d]
    def rem(b,v,d):
        e = hist.get(b)
        if not e: return
        e[0]-=v; e[1]-=d
        if abs(e[0])<1e-9 and abs(e[1])<1e-9: del hist[b]

    left = right = 0; N = len(mins); filled = 0
    out_rows = []
    for r, T in zip(rows, epochs):
        r = list(r) + [""]*(len(header_out)-len(r))
        if T is None:
            for c in new_cols: r[out_idx[c]] = ""
            out_rows.append(r); continue
        while right < N and mins[right][0] <= T:
            _,b,v,d = mins[right]; add(b,v,d); right += 1
        while left < right and mins[left][0] <= T - W:
            _,b,v,d = mins[left]; rem(b,v,d); left += 1
        close = None
        try: close = float(r[idx["close"]])
        except: pass
        vals = profile_fields(hist, close)
        for c, val in zip(new_cols, vals): r[out_idx[c]] = "" if val=="" else val
        if vals[0] != "": filled += 1
        out_rows.append(r)

    tmp = csv_path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(header_out); w.writerows(out_rows)
    os.replace(tmp, csv_path)

    print(f"[OK] {csv_path}")
    print(f"     kolom: {'+'.join(c for c in new_cols if c not in idx) or '(overwrite)'}  | baris: {len(out_rows)}  | terisi: {filled}")
    if out_rows:
        last = out_rows[-1]; g = lambda c: last[out_idx[c]]
        print(f"     contoh akhir: close {last[idx['close']]}  POC {g(PREFIX+'poc')}  VAH {g(PREFIX+'vah')}  "
              f"VAL {g(PREFIX+'val')}  pos {g(PREFIX+'pos')}  Δ {g(PREFIX+'delta')}  pocΔ {g(PREFIX+'poc_delta')}  dist {g(PREFIX+'poc_dist')}%")

# ---------------- selftest (tanpa jaringan) ----------------
def selftest():
    # profil sintetis: gunung volume di 60000, ekor tipis 59000-61000, sedikit di 58000
    hist = {}
    for b, vol, dlt in [
        (58000//25,  50,  -20),
        (59500//25, 400,  -60),
        (59750//25, 900, -150),   # area tebal bawah POC
        (60000//25,1500,   80),   # POC (delta +)
        (60250//25, 700,  -40),
        (60500//25, 160,  -30),   # whale-ask-ish
        (61000//25,  40,   10),
    ]:
        hist[b] = [float(vol), float(dlt)]
    poc,vah,val,td,pd_,pos,dist = profile_fields(hist, close=60400.0)
    print("=== SELFTEST profile_fields ===")
    print(f" POC={poc} (harap ~60000)  VAH={vah}  VAL={val}")
    print(f" total_delta={td}  poc_delta={pd_} (harap +80)")
    print(f" close=60400 -> pos={pos} (harap above_vah jika VAH<60400, else in_va)  dist={dist}%")
    ok = (abs(poc-60000)<1e-6 and abs(pd_-80)<1e-6)
    print(" RESULT:", "OK ✅" if ok else "CEK ❌")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest(); sys.exit(0)
    csv_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(BASE, "btc_merged_hourly.csv")
    if len(sys.argv) > 2: WINDOW_H = int(sys.argv[2])
    if len(sys.argv) > 3: BIN_USD  = float(sys.argv[3])
    if len(sys.argv) > 4: PREFIX   = sys.argv[4]
    try:
        main(csv_path)
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"\n[FATAL] {type(e).__name__}: {e}\n[INFO] CSV lama TIDAK diubah (atomic .tmp).")
        sys.exit(1)
