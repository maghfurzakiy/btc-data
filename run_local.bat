@echo off
setlocal
cd /d "%~dp0"

set "PY=python"
where python >nul 2>nul || set "PY=py"
where %PY% >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python tidak ditemukan. Install dari https://python.org dan centang Add to PATH.
  pause
  exit /b 1
)

where git >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Git tidak ditemukan. Install dari https://git-scm.com
  pause
  exit /b 1
)

echo [1/9] Sinkron arsip dari cloud (merge + union, anti-konflik) ...
git pull --no-rebase --autostash --no-edit
if errorlevel 1 (
  echo [ERROR] Sinkron gagal/konflik tak terduga. Jalankan:  git merge --abort  lalu coba lagi.
  pause
  exit /b 1
)

echo [2/9] Menangkap data segar lalu regenerate ...
%PY% build_merged_dataset.py
set "RC=%errorlevel%"

if not "%RC%"=="0" (
  echo.
  echo ============================================
  echo [BATAL] Builder gagal kode %RC% -- TIDAK commit/push.
  echo CSV lama tetap utuh. Cek pesan error di atas, lalu jalankan lagi.
  echo ============================================
  pause
  exit /b %RC%
)

echo [3/9] Sinyal Tier-1 (tz/ecc/er/rv) -- NOL network, dijamin jalan ...
%PY% add_signals.py
if errorlevel 1 (
  echo [WARN] add_signals gagal -- CSV tetap valid tanpa kolom sinyal. Lanjut.
)

echo [4/9] Regime classifier (UP/DOWN/CHOP) -- NOL network, dijamin jalan ...
%PY% add_regime.py
if errorlevel 1 (
  echo [WARN] add_regime gagal -- CSV tetap valid tanpa kolom regime. Lanjut.
)

echo [5/9] Volume Profile / AMT (POC/VAH/VAL/delta) ...
%PY% add_volume_profile.py
if errorlevel 1 (
  echo [WARN] add_volume_profile gagal -- CSV tetap valid tanpa kolom vp_. Lanjut.
  echo        ^(Sumber 1m Binance Vision mungkin sedang down; coba lagi nanti.^)
)

echo [6/9] Volume Profile 96h (swing, vp96_) ...
%PY% add_volume_profile.py btc_merged_hourly.csv 96 25 vp96_
if errorlevel 1 (
  echo [WARN] add_volume_profile 96h gagal -- CSV tetap valid tanpa kolom vp96_. Lanjut.
)

echo [7/9] CVD spot + divergence + basis (#4 + #6) ...
%PY% add_spot_cvd.py
if errorlevel 1 (
  echo [WARN] add_spot_cvd gagal -- CSV tetap valid tanpa kolom spot/basis. Lanjut.
  echo        ^(Sumber spot 1m Binance Vision mungkin sedang down; coba lagi nanti.^)
)

echo [8/9] Commit perubahan ...
git add btc_merged_hourly.csv upnl_history.csv cache
git diff --staged --quiet
if errorlevel 1 (
  git commit -m "local capture %DATE% %TIME%"
) else (
  echo Tak ada perubahan untuk di-commit.
)

echo [9/9] Push ke cloud ...
git push
if errorlevel 1 (
  echo Push ditolak - sinkron ulang lalu coba lagi ...
  git pull --no-rebase --autostash --no-edit
  git push
)

echo ============================================
echo Selesai. btc_merged_hourly.csv siap diupload ke Claude.
echo ============================================
echo.
pause
