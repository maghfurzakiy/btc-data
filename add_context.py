#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
add_context.py — enricher KONTEKS-SNAPSHOT (ganti nama add_options.py, diperluas).

Satu file utk semua data POINT-IN-TIME eksternal (beda dari per-row recomputer spt vp_/regime).
Tiap sumber = fungsi snap_* sendiri, try/except terisolasi, cache sendiri, prefix kolom sendiri.
Semua di-stamp ke btc_merged_hourly.csv lewat cache->join by-jam (akumulatif ke depan).

  SUMBER                PREFIX   CACHE
  Deribit option-chain    opt_    cache/opt_snapshot.csv   (GEX/max-pain/PCR/gamma-flip/IV)
  Checkonchain MRI        oc_     cache/oc_snapshot.csv    (on-chain valuation + mean-quantile)
  CoinMarketCap resmi     cmc_    cache/cmc_snapshot.csv   (Fear&Greed CMC + BTC/ETH dominance + mcap)

POST-PROCESSOR (pola add_volume_profile.py): baca CSV, APPEND kolom di AKHIR, atomic write,
idempoten (overwrite kalau sudah ada). STDLIB murni. Satu langkah run_local (ganti add_options).

=== KENAPA HITUNG/AMBIL SENDIRI ===
  - Options: GEX/max-pain = turunan chain Deribit (publik, no-key, ~90% OI). Ganti screenshot Laevitas/CG.
  - On-chain: Checkonchain nyimpen data trace sebagai Plotly-JSON EMBEDDED di HTML statik (bukan API).
    Kita fetch HTML -> extract figure -> baca y-terakhir tiap trace. Ganti screenshot Mean-Reversion-Index.
    CAVEAT: scraping HTML statik (rapuh kalau format export berubah); cadence DAILY/makro; data Glassnode-derived.
  - CMC: pakai Trial Pro API RESMI keyless (pro-api.coinmarketcap.com/trial-pro-api, GET-only,
    rate-limit ketat -> aman utk 2 call/jam). Endpoint: /v3/fear-and-greed/latest + /v1/global-metrics/quotes/latest.
    Opsional env CMC_API_KEY (free Basic 15k credit/bln) -> otomatis pindah ke pro-api berkunci.
    "Price vs F&G": harga SUDAH ada di CSV (kolom close) -> begitu cmc_fg ke-stamp per-jam,
    series price-vs-FNG kebentuk sendiri utk backtest. Ganti screenshot CMC Fear&Greed.

=== SEMANTIK: SNAPSHOT (bukan historis) ===
  Kedua sumber = keadaan SEKARANG. Di-stamp ke cache per-jam (last-write-wins) lalu JOIN by-jam ke CSV.
  Baris sebelum run pertama = KOSONG (jujur; histori butuh sumber berbayar). Akumulatif tiap run.

=== KOLOM opt_ (10) ===  spot, gex_total($M/1%,bertanda), gex_flip, pin, call_wall, put_wall, max_pain, pcr_oi, iv_atm, oi_total
=== KOLOM oc_  (17) ===  mean_quantile, mri, mri_spread/ceiling/floor/fast/slow, price, realized, true_mean, sth_cost, cointime, vwap90, vwap365, powerlaw, 200wma, 200dma
=== KOLOM cmc_ (5)  ===  fg (0-100), fg_class, btc_dom (%), eth_dom (%), mcap_t ($T)

=== MODELING CHOICE (options) ===  DEALER_LONG_CALLS=True -> net GEX = callGEX - putGEX. Flip ke False utk konvensi kebalikan.

PEMAKAIAN
  python add_context.py                     # default btc_merged_hourly.csv; kedua sumber
  python add_context.py --dry               # fetch+print, TIDAK tulis CSV/cache
  python add_context.py --greeks            # options: gamma ASLI ticker (auto kalau mark_iv coverage<50%)
  python add_context.py --only options      # cuma satu sumber (options|onchain|cmc)
  python add_context.py --selftest          # uji math options + parser on-chain tanpa jaringan
"""
import json, csv, os, io, sys, time, math, re, urllib.request
from datetime import datetime, timezone

SYMBOL   = "BTC"
BASE     = os.path.dirname(os.path.abspath(__file__))
CACHE    = os.path.join(BASE, "cache"); os.makedirs(CACHE, exist_ok=True)
OPT_SNAP = os.path.join(CACHE, "opt_snapshot.csv")
OC_SNAP  = os.path.join(CACHE, "oc_snapshot.csv")
CMC_SNAP = os.path.join(CACHE, "cmc_snapshot.csv")
DERIBIT  = "https://www.deribit.com/api/v2/public"
CHECKONCHAIN_URL = "https://charts.checkonchain.com/btconchain/pricing/meanreversion_index/meanreversion_index_light.html"
# CMC: Trial Pro API = KEYLESS resmi (GET-only, rate-limit ketat -> cukup utk snapshot per-jam).
# Kalau env CMC_API_KEY di-set (free Basic 15k credit/bln) -> pakai pro-api berkunci (limit longgar).
CMC_TRIAL = "https://pro-api.coinmarketcap.com/trial-pro-api"
CMC_PRO   = "https://pro-api.coinmarketcap.com"
UA       = {"User-Agent": "Mozilla/5.0"}

DEALER_LONG_CALLS = True
RISK_FREE         = 0.0
MIN_T_YEARS       = 1/8760
YEAR_SEC          = 365.0 * 24 * 3600

NEW_COLS_OPT = ["opt_spot", "opt_gex_total", "opt_gex_flip", "opt_pin", "opt_call_wall",
                "opt_put_wall", "opt_max_pain", "opt_pcr_oi", "opt_iv_atm", "opt_oi_total"]
NEW_COLS_OC  = ["oc_mean_quantile", "oc_mri", "oc_mri_spread", "oc_mri_ceiling", "oc_mri_floor",
                "oc_mri_fast", "oc_mri_slow", "oc_price", "oc_realized", "oc_true_mean",
                "oc_sth_cost", "oc_cointime", "oc_vwap90", "oc_vwap365", "oc_powerlaw",
                "oc_200wma", "oc_200dma"]
NEW_COLS_CMC = ["cmc_fg", "cmc_fg_class", "cmc_btc_dom", "cmc_eth_dom", "cmc_mcap_t"]

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

# ================================================================= OPTIONS (Deribit)
def deribit_chain():
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
    raw = _fetch(urllib.request.Request(f"{DERIBIT}/ticker?instrument_name={instrument}", headers=UA))
    r = json.loads(raw.decode()).get("result", {})
    gk = r.get("greeks") or {}
    return {"gamma": fnum(gk.get("gamma")), "iv": fnum(r.get("mark_iv")),
            "u": fnum(r.get("underlying_price")), "oi": fnum(r.get("open_interest"))}

def _norm_pdf(x): return math.exp(-0.5*x*x) / math.sqrt(2.0*math.pi)
def bs_gamma(S, K, T, sigma, r=0.0):
    if not (S > 0 and K > 0 and T > 0 and sigma > 0):
        return 0.0
    d1 = (math.log(S/K) + (r + 0.5*sigma*sigma)*T) / (sigma*math.sqrt(T))
    return _norm_pdf(d1) / (S * sigma * math.sqrt(T))

def parse_instrument(name):
    try:
        _, ds, strike, cp = name.split("-")
        cp = cp.upper()
        if cp not in ("C", "P"): return None
        mon = MONTHS[ds[-5:-2]]
        day = int(ds[:-5]); yy = 2000 + int(ds[-2:])
        exp = datetime(yy, mon, day, 8, 0, 0, tzinfo=timezone.utc)
        return (int(exp.timestamp()), float(strike), cp)
    except Exception:
        return None

def compute_snapshot(chain, now_ts, spot_hint=None, greeks_map=None):
    greeks_map = greeks_map or {}
    opts = []; und = []
    for it in chain:
        name = it.get("instrument_name", "")
        p = parse_instrument(name)
        if not p: continue
        exp, K, cp = p
        oi = fnum(it.get("open_interest")); u = fnum(it.get("underlying_price"))
        if u: und.append(u)
        if oi is None or oi <= 0: continue
        gm = greeks_map.get(name) or {}
        iv = gm.get("iv") if gm.get("iv") is not None else fnum(it.get("mark_iv"))
        o = {"exp": exp, "K": K, "cp": cp, "oi": oi,
             "iv": (iv/100.0 if iv is not None else None), "u": u, "vol": fnum(it.get("volume"))}
        if gm.get("gamma") is not None: o["gamma"] = gm["gamma"]
        opts.append(o)
    if not opts: return None
    iv_cov = round(sum(1 for o in opts if o.get("gamma") is not None or o["iv"] is not None)/len(opts)*100, 0)
    S = spot_hint or (sorted(und)[len(und)//2] if und else None)
    if S is None: return None

    gex_strike = {}; net_gex = 0.0
    for o in opts:
        g = o.get("gamma")
        if g is None:
            T = max((o["exp"] - now_ts) / YEAR_SEC, MIN_T_YEARS)
            g = bs_gamma(S, o["K"], T, o["iv"], RISK_FREE) if o["iv"] else 0.0
        sign = 1.0 if o["cp"] == "C" else -1.0
        if not DEALER_LONG_CALLS: sign = -sign
        gex = g * o["oi"] * S * S * 0.01 * sign
        net_gex += gex
        gex_strike[o["K"]] = gex_strike.get(o["K"], 0.0) + gex

    strikes = sorted(gex_strike)
    flip = None; cum = 0.0; prev_k = None; prev_cum = 0.0
    for k in strikes:
        cum += gex_strike[k]
        if prev_k is not None and (prev_cum < 0 <= cum or prev_cum > 0 >= cum):
            span = cum - prev_cum
            flip = prev_k + (k - prev_k) * ((0 - prev_cum)/span if span else 0.5); break
        prev_k, prev_cum = k, cum
    near = [k for k in strikes if abs(k - S)/S <= 0.10]
    pin = max(near, key=lambda k: gex_strike[k]) if near else None
    above = [k for k in strikes if k > S]; below = [k for k in strikes if k < S]
    call_wall = max(above, key=lambda k: gex_strike[k]) if above else None
    put_wall  = min(below, key=lambda k: gex_strike[k]) if below else None

    call_oi = sum(o["oi"] for o in opts if o["cp"] == "C")
    put_oi  = sum(o["oi"] for o in opts if o["cp"] == "P")
    pcr = round(put_oi/call_oi, 3) if call_oi > 0 else ""
    oi_total = round(call_oi + put_oi, 1)

    exps = sorted(set(o["exp"] for o in opts if o["exp"] > now_ts)); max_pain = ""
    for e in exps:
        grp = [o for o in opts if o["exp"] == e]
        if sum(o["oi"] for o in grp) < 50: continue
        ks = sorted(set(o["K"] for o in grp))
        def payout(P): return sum(o["oi"]*max(0.0, (P-o["K"]) if o["cp"]=="C" else (o["K"]-P)) for o in grp)
        max_pain = min(ks, key=payout); break

    iv_atm = ""
    if exps:
        cand = [o for o in opts if o["exp"] == exps[0] and o["iv"]]
        if cand:
            a = min(cand, key=lambda o: abs(o["K"] - S)); iv_atm = round(a["iv"]*100.0, 1)

    return {"opt_spot": round(S,1), "opt_gex_total": round(net_gex/1e6,2),
            "opt_gex_flip": ("" if flip is None else round(flip,0)),
            "opt_pin": ("" if pin is None else round(pin,0)),
            "opt_call_wall": ("" if call_wall is None else round(call_wall,0)),
            "opt_put_wall": ("" if put_wall is None else round(put_wall,0)),
            "opt_max_pain": ("" if max_pain == "" else round(max_pain,0)),
            "opt_pcr_oi": pcr, "opt_iv_atm": iv_atm, "opt_oi_total": oi_total, "_iv_cov": iv_cov}

def snap_options(now_ts, use_greeks=False):
    """Fetch chain Deribit + compute. -> dict NEW_COLS_OPT (tanpa _iv_cov) atau None."""
    chain = deribit_chain()
    spot = deribit_index()
    if spot is None:
        u = [fnum(it.get("underlying_price")) for it in chain if fnum(it.get("underlying_price"))]
        spot = sorted(u)[len(u)//2] if u else None
    have_iv = sum(1 for it in chain if fnum(it.get("mark_iv")) is not None
                  and (fnum(it.get("open_interest")) or 0) > 0 and parse_instrument(it.get("instrument_name","")))
    tot = sum(1 for it in chain if (fnum(it.get("open_interest")) or 0) > 0 and parse_instrument(it.get("instrument_name","")))
    cov0 = (have_iv/tot*100) if tot else 0
    greeks_map = {}
    if (use_greeks or cov0 < 50) and spot:
        near = [it for it in chain
                if (fnum(it.get("open_interest")) or 0) > 0 and parse_instrument(it.get("instrument_name",""))
                and abs(parse_instrument(it["instrument_name"])[1]-spot)/spot <= 0.25]
        why = "flag --greeks" if use_greeks else f"mark_iv coverage rendah ({cov0:.0f}%)"
        print(f"  [greeks] {why} -> ticker gamma-asli utk {len(near)} instrumen near-money ...")
        for k, it in enumerate(near):
            nm = it["instrument_name"]
            try: greeks_map[nm] = deribit_ticker(nm)
            except Exception as e: print(f"    warn ticker {nm}: {type(e).__name__}")
            if k and k % 40 == 0: time.sleep(0.3)
    snap = compute_snapshot(chain, now_ts, spot_hint=spot, greeks_map=greeks_map)
    if snap is None: return None
    print(f"  [options] chain {len(chain)} | IV/gamma cov {snap['_iv_cov']:.0f}% | "
          f"GEX {snap['opt_gex_total']}M ({'NET-LONG' if snap['opt_gex_total']>=0 else 'NET-SHORT'}) "
          f"pin {snap['opt_pin']} maxpain {snap['opt_max_pain']} pcr {snap['opt_pcr_oi']}")
    return {k: snap[k] for k in NEW_COLS_OPT}

# ================================================================= ON-CHAIN (Checkonchain)
def _match_bracket(s, i):
    """s[i]='[' atau '{'; return indeks tepat setelah penutup yang cocok (hormati string). -1 kalau gagal."""
    open_ch = s[i]; close_ch = "]" if open_ch == "[" else "}"
    depth = 0; in_str = False; esc = False; q = ""
    j = i
    while j < len(s):
        c = s[j]
        if in_str:
            if esc: esc = False
            elif c == "\\": esc = True
            elif c == q: in_str = False
        else:
            if c == '"' or c == "'": in_str = True; q = c
            elif c == open_ch: depth += 1
            elif c == close_ch:
                depth -= 1
                if depth == 0: return j + 1
        j += 1
    return -1

def extract_plotly_figure(html):
    """Ambil {'data':[...],'layout':{...}} dari HTML Plotly statik. Coba 2 pola umum."""
    # Pola A: <script type="application/json"> berisi figure {data,layout}
    for m in re.finditer(r'<script[^>]*type="application/json"[^>]*>(.*?)</script>', html, re.S):
        try:
            obj = json.loads(m.group(1).strip())
            if isinstance(obj, dict) and "data" in obj: return obj
            if isinstance(obj, dict) and "x" in obj and isinstance(obj["x"], dict) and "data" in obj["x"]:
                return obj["x"]   # htmlwidgets wrap
        except Exception:
            pass
    # Pola B: Plotly.newPlot("id", DATA, LAYOUT, ...)
    idx = html.find("Plotly.newPlot(")
    if idx != -1:
        b = html.find("[", idx)
        if b != -1:
            end = _match_bracket(html, b)
            if end != -1:
                data = json.loads(html[b:end])
                lb = html.find("{", end); layout = {}
                if lb != -1:
                    le = _match_bracket(html, lb)
                    if le != -1:
                        try: layout = json.loads(html[lb:le])
                        except Exception: layout = {}
                return {"data": data, "layout": layout}
    raise RuntimeError("figure Plotly tak ketemu di HTML (format export mungkin berubah)")

def oc_col_for(name):
    n = (name or "").strip().lower()
    exact = {"index": "oc_mri", "index_spread": "oc_mri_spread",
             "ceiling index": "oc_mri_ceiling", "floor index": "oc_mri_floor",
             "fast index": "oc_mri_fast", "slow index": "oc_mri_slow",
             "price": "oc_price", "powerlaw": "oc_powerlaw", "cointime price": "oc_cointime",
             "sth cost basis": "oc_sth_cost", "true market mean": "oc_true_mean",
             "realized price": "oc_realized", "90d-onchain vwap": "oc_vwap90",
             "365d-onchain vwap": "oc_vwap365", "200wma": "oc_200wma", "200dma": "oc_200dma"}
    if n in exact: return exact[n]
    subs = [("spread","oc_mri_spread"),("ceiling","oc_mri_ceiling"),("floor","oc_mri_floor"),
            ("fast","oc_mri_fast"),("slow","oc_mri_slow"),("powerlaw","oc_powerlaw"),
            ("cointime","oc_cointime"),("sth","oc_sth_cost"),("true market","oc_true_mean"),
            ("realized","oc_realized"),("90d","oc_vwap90"),("365d","oc_vwap365"),
            ("200wma","oc_200wma"),("200dma","oc_200dma")]
    for k, c in subs:
        if k in n: return c
    return None

def _decode_plotly_array(y):
    """Plotly baru encode array numerik sbg {'dtype':'f8','bdata':base64}. Decode -> list float.
       dtype: f8=float64, f4=float32, i8/i4/u.. -> pakai struct. Non-dict (list biasa) diloloskan."""
    if isinstance(y, list):
        return y
    if isinstance(y, dict) and "bdata" in y:
        import base64, struct
        raw = base64.b64decode(y["bdata"])
        fmt = {"f8": ("<d", 8), "f4": ("<f", 4),
               "i8": ("<q", 8), "i4": ("<i", 4), "u8": ("<Q", 8), "u4": ("<I", 4),
               "i2": ("<h", 2), "u2": ("<H", 2), "i1": ("<b", 1), "u1": ("<B", 1)}
        code, size = fmt.get(str(y.get("dtype", "f8")), ("<d", 8))
        n = len(raw)//size
        return [struct.unpack_from(code, raw, k*size)[0] for k in range(n)]
    return None

def _last_finite(y):
    arr = _decode_plotly_array(y)
    if not isinstance(arr, list): return None
    for v in reversed(arr):
        if isinstance(v, (int, float)) and v == v and v not in (float("inf"), float("-inf")):
            return float(v)
    return None

def parse_onchain(html):
    """HTML Checkonchain -> dict oc_ (dari trace by-name + mean_quantile dari teks)."""
    fig = extract_plotly_figure(html)
    out = {c: "" for c in NEW_COLS_OC}
    for tr in fig.get("data", []):
        col = oc_col_for(tr.get("name", ""))
        if not col or col not in out or out[col] != "": continue
        v = _last_finite(tr.get("y"))
        if v is not None:
            out[col] = round(v, 1) if abs(v) < 1000 else int(round(v))
    # mean quantile: teks anotasi "Mean Quantile: 13.6%"
    mq = re.search(r"Mean\s*Quantile[:\s]*([0-9]+(?:\.[0-9]+)?)", html, re.I)
    if mq: out["oc_mean_quantile"] = float(mq.group(1))
    return out

def snap_onchain(now_ts):
    """Fetch Checkonchain HTML + parse. -> dict NEW_COLS_OC atau None."""
    raw = _fetch(urllib.request.Request(CHECKONCHAIN_URL, headers=UA))
    html = raw.decode("utf-8", errors="replace")
    oc = parse_onchain(html)
    filled = sum(1 for c in NEW_COLS_OC if oc.get(c) not in ("", None))
    if filled == 0: return None
    print(f"  [onchain] trace terisi {filled}/{len(NEW_COLS_OC)} | quantile {oc['oc_mean_quantile']} "
          f"| realized {oc['oc_realized']} true_mean {oc['oc_true_mean']} sth {oc['oc_sth_cost']}")
    return oc

# ================================================================= CMC (Fear&Greed + dominance)
def parse_cmc_fg(obj):
    """respons /v3/fear-and-greed/latest -> (value, classification). Tahan bentuk dict/list."""
    d = obj.get("data")
    if isinstance(d, list): d = d[0] if d else {}
    if not isinstance(d, dict): return (None, "")
    v = fnum(d.get("value"))
    cls = d.get("value_classification") or ""
    return (v, cls)

def parse_cmc_global(obj):
    """respons /v1/global-metrics/quotes/latest -> (btc_dom, eth_dom, mcap_$T)."""
    d = obj.get("data") or {}
    btc = fnum(d.get("btc_dominance")); eth = fnum(d.get("eth_dominance"))
    mcap = None
    q = d.get("quote") or {}
    usd = q.get("USD") or {}
    mcap = fnum(usd.get("total_market_cap"))
    return (btc, eth, (round(mcap/1e12, 3) if mcap else None))

def _cmc_get(path):
    """GET CMC: pakai key (env CMC_API_KEY) kalau ada, else Trial keyless."""
    key = os.environ.get("CMC_API_KEY", "").strip()
    if key:
        req = urllib.request.Request(CMC_PRO + path, headers={**UA, "X-CMC_PRO_API_KEY": key})
    else:
        req = urllib.request.Request(CMC_TRIAL + path, headers=UA)
    return json.loads(_fetch(req).decode())

def snap_cmc(now_ts):
    """F&G resmi CMC + BTC/ETH dominance + total mcap. -> dict NEW_COLS_CMC atau None."""
    out = {c: "" for c in NEW_COLS_CMC}
    try:
        v, cls = parse_cmc_fg(_cmc_get("/v3/fear-and-greed/latest"))
        if v is not None: out["cmc_fg"] = round(v, 0); out["cmc_fg_class"] = cls
    except Exception as e:
        print(f"    warn cmc F&G: {type(e).__name__}: {e}")
    time.sleep(1.2)   # santun ke rate-limit trial
    try:
        btc, eth, mcap = parse_cmc_global(_cmc_get("/v1/global-metrics/quotes/latest"))
        if btc is not None: out["cmc_btc_dom"] = round(btc, 2)
        if eth is not None: out["cmc_eth_dom"] = round(eth, 2)
        if mcap is not None: out["cmc_mcap_t"] = mcap
    except Exception as e:
        print(f"    warn cmc global: {type(e).__name__}: {e}")
    filled = sum(1 for c in NEW_COLS_CMC if out[c] not in ("", None))
    if filled == 0: return None
    print(f"  [cmc] F&G {out['cmc_fg']} ({out['cmc_fg_class']}) | BTC.D {out['cmc_btc_dom']}% "
          f"ETH.D {out['cmc_eth_dom']}% | mcap ${out['cmc_mcap_t']}T "
          f"({'keyed' if os.environ.get('CMC_API_KEY') else 'trial keyless'})")
    return out

# ================================================================= cache + join
def floorH(ts): return ts - (ts % 3600)

def load_snap_cache(path, cols):
    rows = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                try: h = int(r["hour_ts"])
                except Exception: continue
                rows[h] = {k: r.get(k, "") for k in cols}
    return rows

def save_snap_cache(path, cache, cols):
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["hour_ts"] + cols)
        for h in sorted(cache):
            w.writerow([h] + [cache[h].get(c, "") for c in cols])
    os.replace(tmp, path)

def epoch(s):
    try: return int(datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp())
    except Exception: return None

# ================================================================= main (orchestrator)
def main(path, dry=False, use_greeks=False, only=None):
    print(f"[{datetime.now(timezone.utc).isoformat()}] add_context  (options Deribit + on-chain Checkonchain + CMC F&G/dominance)")
    now_ts = int(datetime.now(timezone.utc).timestamp())

    # --- kumpulkan snapshot per sumber (terisolasi: satu gagal, lain jalan) ---
    sources = []   # (cache_path, cols, snapdict)
    if only in (None, "options"):
        try:
            d = snap_options(now_ts, use_greeks)
            if d: sources.append((OPT_SNAP, NEW_COLS_OPT, d))
            else: print("  [options] kosong — dilewati.")
        except Exception as e:
            print(f"  [WARN] options gagal ({type(e).__name__}: {e}) — dilewati.")
    if only in (None, "onchain"):
        try:
            d = snap_onchain(now_ts)
            if d: sources.append((OC_SNAP, NEW_COLS_OC, d))
            else: print("  [onchain] kosong — dilewati.")
        except Exception as e:
            print(f"  [WARN] onchain gagal ({type(e).__name__}: {e}) — dilewati.")
    if only in (None, "cmc"):
        try:
            d = snap_cmc(now_ts)
            if d: sources.append((CMC_SNAP, NEW_COLS_CMC, d))
            else: print("  [cmc] kosong — dilewati.")
        except Exception as e:
            print(f"  [WARN] cmc gagal ({type(e).__name__}: {e}) — dilewati.")

    if not sources:
        print("[ERR] semua sumber gagal. CSV TIDAK diubah."); sys.exit(1)
    if dry:
        print("  [dry] tidak menulis CSV/cache."); return

    # --- stamp tiap cache (last-write-wins per jam) ---
    for cache_path, cols, d in sources:
        cache = load_snap_cache(cache_path, cols)
        cache[floorH(now_ts)] = {k: d.get(k, "") for k in cols}
        save_snap_cache(cache_path, cache, cols)
        print(f"  cache {os.path.basename(cache_path)}: {len(cache)} jam")

    # --- join SEMUA kolom (opt_ + oc_) ke CSV by-jam ---
    if not os.path.exists(path):
        print(f"[ERR] file merged tak ada: {path}"); sys.exit(1)
    with open(path, encoding="utf-8", newline="") as f:
        rd = csv.reader(f); header = next(rd); rows = list(rd)
    idx = {c: i for i, c in enumerate(header)}
    if "timestamp_utc" not in idx:
        print("[ERR] kolom timestamp_utc tak ada — bukan CSV builder."); sys.exit(1)

    all_cols = NEW_COLS_OPT + NEW_COLS_OC + NEW_COLS_CMC
    caches = {cache_path: load_snap_cache(cache_path, cols) for cache_path, cols, _ in
              [(OPT_SNAP, NEW_COLS_OPT, None), (OC_SNAP, NEW_COLS_OC, None), (CMC_SNAP, NEW_COLS_CMC, None)]}
    header_out = header + [c for c in all_cols if c not in idx]
    out_idx = {c: i for i, c in enumerate(header_out)}
    fill = {c: 0 for c in all_cols}
    out_rows = []
    for r in rows:
        r = list(r) + [""]*(len(header_out)-len(r))
        T = epoch(r[idx["timestamp_utc"]]); h = floorH(T) if T is not None else None
        recO = caches[OPT_SNAP].get(h) if h is not None else None
        recC = caches[OC_SNAP].get(h) if h is not None else None
        recM = caches[CMC_SNAP].get(h) if h is not None else None
        for c in NEW_COLS_OPT:
            v = recO.get(c, "") if recO else ""; r[out_idx[c]] = v
            if v not in ("", None): fill[c] += 1
        for c in NEW_COLS_OC:
            v = recC.get(c, "") if recC else ""; r[out_idx[c]] = v
            if v not in ("", None): fill[c] += 1
        for c in NEW_COLS_CMC:
            v = recM.get(c, "") if recM else ""; r[out_idx[c]] = v
            if v not in ("", None): fill[c] += 1
        out_rows.append(r)

    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(header_out); w.writerows(out_rows)
    os.replace(tmp, path)

    appended = [c for c in all_cols if c not in idx]
    print(f"[OK] {path}")
    print(f"     kolom: {'+'.join(appended) if appended else '(sudah ada, overwrite)'}  | baris: {len(out_rows)}")
    for c in all_cols:
        if fill[c]: print(f"     {c:16s} terisi {fill[c]}/{len(out_rows)}")
    print("     (kolom snapshot: terisi hanya jam yang pernah di-run — akumulatif ke depan)")

# ================================================================= selftest
def selftest():
    print("=== SELFTEST add_context ===")
    # --- OPTIONS math ---
    S = 60000.0
    g_atm = bs_gamma(S, 60000, 30/365, 0.5); g_otm = bs_gamma(S, 70000, 30/365, 0.5)
    now = int(datetime.now(timezone.utc).timestamp()); exp = now + 7*24*3600
    ds = datetime.fromtimestamp(exp, timezone.utc)
    tag = f"{ds.day}{['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'][ds.month-1]}{str(ds.year)[2:]}"
    chain = []
    def add(K, cp, oi, iv):
        chain.append({"instrument_name": f"BTC-{tag}-{K}-{cp}", "open_interest": oi,
                      "mark_iv": iv, "underlying_price": 60000.0, "volume": 10})
    add(58000,"P",800,45); add(59000,"P",400,42); add(60000,"C",300,40); add(60000,"P",300,40)
    add(61000,"C",500,41); add(62000,"C",1200,43); add(64000,"C",200,46)
    snap = compute_snapshot(chain, now, spot_hint=60000.0)
    call_oi=300+500+1200+200; put_oi=800+400+300
    opt_ok = (g_atm>g_otm and abs(snap["opt_pcr_oi"]-round(put_oi/call_oi,3))<1e-6
              and snap["opt_max_pain"]!="" and snap["opt_call_wall"] in (61000,62000,64000)
              and snap["opt_put_wall"] in (58000,59000))
    print(f" OPTIONS: gammaATM>OTM={g_atm>g_otm} pcr={snap['opt_pcr_oi']} maxpain={snap['opt_max_pain']} "
          f"call_wall={snap['opt_call_wall']} put_wall={snap['opt_put_wall']} -> {'OK' if opt_ok else 'CEK'}")

    # --- ONCHAIN parser: HTML Plotly sintetis (Plotly.newPlot) ---
    import base64, struct
    # Realized Price sbg TYPED-ARRAY base64 float64 (spt Plotly baru): [nan, 41000, 41500]
    bdata = base64.b64encode(struct.pack("<3d", float("nan"), 41000.0, 41500.0)).decode()
    fig_data = [
        {"name":"Realized Price","x":["a","b","c"],"y":{"dtype":"f8","bdata":bdata}},
        {"name":"True Market Mean","x":["2026-06-30","2026-07-01"],"y":[52000,52200]},
        {"name":"STH Cost Basis","x":["2026-06-30","2026-07-01"],"y":[63000,None]},
        {"name":"Index","x":["2026-06-30","2026-07-01"],"y":[340,352.5]},
        {"name":"Ceiling Index","x":["2026-06-30","2026-07-01"],"y":[400,400]},
        {"name":"200WMA","x":["2026-06-30","2026-07-01"],"y":[62000,62500]},
        {"name":"Price","x":["2026-06-30","2026-07-01"],"y":[60000,60700]},
    ]
    html = ('<html><body><div id="x"></div><script type="text/javascript">'
            'Plotly.newPlot("x", ' + json.dumps(fig_data) + ', {"title":"MRI"}, {});'
            '</script><div>Mean Quantile: 13.6%</div></body></html>')
    oc = parse_onchain(html)
    oc_ok = (oc["oc_realized"]==41500 and oc["oc_true_mean"]==52200 and oc["oc_sth_cost"]==63000
             and oc["oc_mri"]==352.5 and oc["oc_mri_ceiling"]==400 and oc["oc_200wma"]==62500
             and oc["oc_price"]==60700 and oc["oc_mean_quantile"]==13.6)
    print(f" ONCHAIN: realized={oc['oc_realized']} true_mean={oc['oc_true_mean']} sth={oc['oc_sth_cost']}(last-finite) "
          f"mri={oc['oc_mri']} 200wma={oc['oc_200wma']} quantile={oc['oc_mean_quantile']} -> {'OK' if oc_ok else 'CEK'}")
    # --- CMC parser: respons sintetis F&G + global-metrics ---
    fg = parse_cmc_fg({"data":[{"timestamp":"x","value":23,"value_classification":"Fear"}]})
    fg2 = parse_cmc_fg({"data":{"value":71,"value_classification":"Greed"}})
    gl = parse_cmc_global({"data":{"btc_dominance":57.93,"eth_dominance":9.72,
                                   "quote":{"USD":{"total_market_cap":2.31e12}}}})
    cmc_ok = (fg==(23.0,"Fear") and fg2==(71.0,"Greed") and gl[0]==57.93 and gl[1]==9.72 and gl[2]==2.31)
    print(f" CMC: fg(list)={fg} fg(dict)={fg2} dom={gl} -> {'OK' if cmc_ok else 'CEK'}")
    # bracket matcher edge
    bm_ok = _match_bracket('[1,[2,"]"],3]', 0) == 13
    print(f" bracket-matcher (string-aware): {'OK' if bm_ok else 'CEK'}")
    print(" RESULT:", "OK ✅" if (opt_ok and oc_ok and cmc_ok and bm_ok) else "CEK ❌")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest(); sys.exit(0)
    dry = "--dry" in sys.argv
    use_greeks = "--greeks" in sys.argv
    only = None
    if "--only" in sys.argv:
        try: only = sys.argv[sys.argv.index("--only")+1]
        except Exception: only = None
    args = [a for a in sys.argv[1:] if not a.startswith("--") and a not in ("options","onchain","cmc")]
    csv_path = args[0] if args else os.path.join(BASE, "btc_merged_hourly.csv")
    try:
        main(csv_path, dry=dry, use_greeks=use_greeks, only=only)
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"\n[FATAL] {type(e).__name__}: {e}\n[INFO] CSV lama TIDAK diubah (atomic .tmp).")
        sys.exit(1)
