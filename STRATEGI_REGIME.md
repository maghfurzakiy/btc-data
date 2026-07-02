# S7 MC — Strategi × Regime + Glosarium

> Referensi framework BTC/USDT perp. Semua edge **hypothesis-grade**, confound single-regime
> (downtrend Jan–Jun 2026). Metrik: no-lookahead, drift-neutral, walk-forward. Di-generate sesi 1.52.
> **Jalur A diskresioner LEADS; Jalur B sistematik FILTER (B tak pernah veto A).**

---

## 1. TABEL STRATEGI × REGIME

Hasil `edge_regime_matrix.py` (WR = win-rate, fwd = horizon uji). 🟢 ON · 🟡 marginal/bersyarat · 🔴 OFF/rugi · ⚪ sampel<30.

| Strategi | Arah | UP | DOWN | CHOP | fwd |
|---|---|---|---|---|---|
| **ep33_long** | long | 🟢 66% | 🔴 43% (rugi) | 🟢 61% | 24h |
| **ep25deep_long** | long | ⚪ n<30 | 🔴 36% | 🟢 62% | 24h |
| **basisamp_long** | long | ⚪ n<30 | 🟡 49% (+rescue) | 🟢 73% | 24h |
| **vafade96_long** | long | 🔴 41% | 🔴 46% | 🟡 53% | 48h |
| **vafade96_short** | short | 🟢 70% | ⚪ n<30 | 🟢 71% | 48h |

**Filter penyelamat khusus DOWN** (dari uji breakdown, ep33-long di DOWN):

| Filter | WR | mean | Verdict |
|---|---|---|---|
| ep33 level saja | 43,5% | −0,492% | 🔴 rugi |
| ep33 **+ basis<−0,06** | 49,2% | **+0,455%** | 🟢 rescue |
| ep33 + ECC=anti | 20,7% | −1,267% | 🔴 veto keras |
| horizon 6h (vs 24-72h) | 49,4% | −0,100% | 🟡 paling ringan |

**Aturan gating hasil matrix (encode ke framework):**
- ep33/ep25-long: **ON di UP & CHOP**, di DOWN **hanya** kalau `basis<−0,06` **dan** ECC bukan-anti, horizon pendek (~6h).
- basisamp: paling bertaji di **CHOP** (73%); di DOWN jadi penyelamat ep33.
- vafade96: sisi **SHORT** yang kuat (UP/CHOP), sisi long lemah — jangan pakai long-nya kecuali CHOP marginal.
- **ECC=anti = stand-aside** di semua regime (terburuk saat DOWN).

---

## 2. PENJELASAN TIAP STRATEGI

**ep33_long** — Contrarian-long saat cohort "Extremely Profitable" (EP) di BTC lagi berat-short (`ep_btc ≤ 33`). Logika: kalau trader paling profit lagi net-short ekstrem, ruang turun menipis & rawan squeeze naik. Edge LEVEL (aktif selama angka di bawah ambang). Kuat di UP/CHOP; di DOWN = fade tren = lemah tanpa filter.

**ep25deep_long** (crown-jewel ★) — Versi dalam dari ep33: `ep_btc ≤ 25`. Sinyal contrarian lebih ekstrem/langka. Kuat di CHOP.

**basisamp_long** — ep33 **plus** perp diskon ke spot (`basis < −0,06%`). Perp-discount = short lagi fear/capitulate = timing contrarian lebih baik. Amplifier konviksi, bukan sinyal berdiri sendiri. **Sel terkuat di seluruh riset: CHOP 73% WR.**

**vafade96_long / _short** — Fade tepi Value-Area profil **96 jam**. Harga di bawah VAL96 → long; di atas VAH96 → short. Timeframe 96h kunci (24h gagal). Riset: **sisi SHORT jauh lebih kuat** (70% UP/CHOP), long lemah.

**exit_flip40** — Aturan KELUAR (bukan entry): tutup posisi long saat `ep_btc > 40` **atau** 72 jam. Lever EV terbesar di framework — exit lebih penting dari entry untuk Sharpe.

**perp_lead_fade** — Fade gerakan perp yang tak dikonfirmasi spot (`|cvd_div|>300` & spot-flow tipis). Size kecil. Perp-led move = rapuh.

**oi_price** — Continuation: OI naik → ikut arah harga. Trend-following, komplemen edge kontrarian. (Belum di-tag per-regime — kandidat R&D.)

**Aturan eksekusi terkunci** — SL longgar/none (SL ketat MERUSAK, MAE avg −2,68%); sizing FLAT (tier-weighted lebih buruk); risk ≤2%/trade (lev efektif ~0,4x); TP bertingkat pakai USDT-absolut di Binance mobile (bukan %); SL post-TP no-BE/no-trail.

---

## 3. GLOSARIUM (singkatan & istilah)

### Cohort HyperDash
- **EP** (Extremely Profitable) — kelompok trader paling profitable. Perilaku mereka jadi sinyal contrarian.
- **VU** (Very Unprofitable) — kelompok paling rugi. (>62% = konfluensi capitulation; sekarang falsified sbg standalone.)
- **ep_btc** — % LONG cohort EP khusus BTC = `100 × longN/(longN+shortN)`. **Rendah (≤33) = berat-short = sinyal contrarian-long.** ≤25 = "deep" (crown-jewel).
- **ep** — versi agregat (semua aset). Framework pakai **ep_btc** (BTC-isolated), bukan ep agregat.
- **ep_btc_sn** — short-notional ($) cohort EP-BTC. Turun 3 jam beruntun = short ditutup (ECC leg-2).
- **ep_btc_ln** — long-notional ($), diturunkan aljabar dari ep_btc & ep_btc_sn.
- **ep_profPct** — % trader EP yang lagi profit. **Naik = short makin in-profit (belum cover) = ANTI.** Turun = profit short mengkerut = lagi cover (ECC leg-3).

### ECC (sinyal inti timing)
**ECC** = sinyal 3-leg penanda short mulai capitulate/cover — timing kapan contrarian-long jadi valid (ep33 kasih LEVEL, ECC kasih TIMING). State: `fire / anti / partial / watch / standaside`.
- **Gate** — `ep_btc ≤ 33` (kalau tidak, `standaside`).
- **Leg-1** (cohort) — ep_btc NAIK ≥ +2,0pp / 3 jam (cohort flip short→cover).
- **Leg-2** (notional) — ep_btc_sn TURUN 3 jam beruntun (short ditutup).
- **Leg-3** (uPNL) — ep_profPct TURUN (profit short hilang).
- **FIRE** = gate + ketiga leg ON → contrarian-long armed penuh (buka gate floor-long ★ swing).
- **ANTI** = ep_profPct NAIK → short makin nyaman, belum cover → **stand-aside / fade**.
- **watch** = gate ON, timing belum lengkap. **partial** = sebagian leg ON.
- Catatan penting: **sn release 1-bar ≠ fire** (spring uncoil bisa palsu — butuh 3-consec).

### Struktur harga & AMT (Volume Profile)
- **basis** — `close(perp) − close(spot)` [$]. **basis_pct** = % dari spot. **+ = perp premium** (leverage-long ramai). **− = perp discount** (short fear). `< −0,06%` = amplifier ep33.
- **POC** (Point of Control) — harga dengan volume terbesar (garis merah TV). Magnet harga.
- **VAH / VAL** — Value Area High / Low = batas atas/bawah 70% volume.
- **vp_pos / vp96_pos** — posisi harga vs value-area: `above_vah` / `in_va` / `below_val`. Suffix `96` = profil 96 jam (swing); tanpa suffix = 24 jam.
- **VAfade96** — strategi fade tepi VA profil 96h (below_val96→long, above_vah96→short).

### Flow & leverage
- **CVD** (Cumulative Volume Delta) — akumulasi (taker-buy − taker-sell). Slope yang penting, bukan level.
- **cvd_spot_d / cvd_perp_d** — delta taker per-jam di spot / perp. **cvd_div** = perp_d − spot_d. **+ = leverage-led** (perp lebih agresif, rapuh); **− = spot-led** (real demand, lebih sehat).
- **FR** (Funding Rate) — biaya perp. **Otoritatif dari CoinGlass** (kolom CSV STALE). ≤ −0,013% = long-override; ≥ +0,009% = short-override.
- **OI** (Open Interest) — total kontrak terbuka. Naik + harga naik = continuation.
- **tz** — z-score leverage (total notional atas window 72h). **+ = leverage di atas normal** (bahan squeeze/flush).
- **taker_ls_ratio** — rasio taker buy/sell. **FALSIFIED** (sesi 1.52: ρ≈0, WF 3/5) — tidak dipakai.

### Volatilitas & regime
- **er** (Efficiency Ratio, Kaufman) — [0..1]. <0,4 = choppy/range; >0,6 = trending.
- **rv24 / rv96** — realized volatility annualized (24h / 96h). **rv_ts** = rv24/rv96. >1 = vol jangka-pendek elevated.
- **regime** — output classifier: **UP / DOWN / CHOP** (+ WARMUP). Dari MA200 + slope + ADX(14):
  - `ADX < 20` → **CHOP** (tren lemah/range).
  - `harga > MA200 & slope > +0,15%` → **UP**.
  - `harga < MA200 & slope < −0,15%` → **DOWN**.
- **ADX / +DI / −DI** — Average Directional Index (kekuatan tren) + Directional Indicators. ADX>20 = trending; −DI>+DI = tekanan turun.
- **MA200** — moving average 200-jam (~8,3 hari). Di bawahnya = regime bearish.

### Arsitektur framework
- **Jalur A** — analisis diskresioner (whale-walls, orderflow, struktur). **LEADS.**
- **Jalur B** — sinyal sistematik/CSV. **FILTER — tak pernah veto A.**
- **★ crown-jewel** — setup konviksi tertinggi (ep_btc deep ≤25 + konfluensi).
- **liqImbalanceNote** — 1 whale gede (~$70M ≈ 0,04% vol harian, sering churn 1-address) = **context, bukan sinyal**. Jangan upweight.

---

*Sintesis dari framework sendiri, bukan saran finansial.*
