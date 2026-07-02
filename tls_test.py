import ssl, urllib.request
URL = "https://fapi.binance.com/fapi/v1/ping"
UA  = {"User-Agent": "Mozilla/5.0"}

print("[A] urllib (cara lama) ...")
try:
    with urllib.request.urlopen(urllib.request.Request(URL, headers=UA), timeout=30) as r:
        print("    urllib OK ->", r.read())
except Exception as e:
    print(f"    urllib GAGAL -> {type(e).__name__}: {e}")

print("[B] requests + TLS-adapter (fix) ...")
try:
    import requests
    from requests.adapters import HTTPAdapter
    class _TLSAdapter(HTTPAdapter):
        def init_poolmanager(self, *a, **k):
            ctx = ssl.create_default_context()
            ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0)
            ctx.options &= ~getattr(ssl, "OP_NO_RENEGOTIATION", 0)
            k["ssl_context"] = ctx
            return super().init_poolmanager(*a, **k)
    s = requests.Session(); s.mount("https://", _TLSAdapter())
    r = s.get(URL, headers=UA, timeout=30); r.raise_for_status()
    print("    requests OK ->", r.content)
    print(">>> FIX BEKERJA. Aman replace 3 file.")
except Exception as e:
    print(f"    requests GAGAL -> {type(e).__name__}: {e}")
    print(">>> Adapter belum cukup -> kabari, kita coba TLS1.3.")