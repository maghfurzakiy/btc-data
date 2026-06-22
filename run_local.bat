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

echo [1/4] Sinkron arsip dari cloud (merge + union, anti-konflik) ...
git pull --no-rebase --autostash --no-edit
if errorlevel 1 (
  echo [ERROR] Sinkron gagal/konflik tak terduga. Jalankan:  git merge --abort  lalu coba lagi.
  pause
  exit /b 1
)

echo [2/4] Menangkap data segar lalu regenerate ...
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

echo [3/4] Commit perubahan ...
git add btc_merged_hourly.csv upnl_history.csv cache
git diff --staged --quiet
if errorlevel 1 (
  git commit -m "local capture %DATE% %TIME%"
) else (
  echo Tak ada perubahan untuk di-commit.
)

echo [4/4] Push ke cloud ...
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
