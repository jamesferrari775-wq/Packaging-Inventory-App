@echo off
setlocal
cd /d "%~dp0"

set "PY_CMD="
where py >nul 2>nul
if %errorlevel%==0 set "PY_CMD=py"
if not defined PY_CMD (
  where python >nul 2>nul
  if %errorlevel%==0 set "PY_CMD=python"
)

if not defined PY_CMD (
  echo.
  echo Could not find Python launcher ^(py^) or python on PATH.
  pause
  exit /b 1
)

echo Installing/updating required packages...
%PY_CMD% -m pip install -r requirements.txt
if errorlevel 1 (
  echo.
  echo Failed to install dependencies.
  pause
  exit /b 1
)

echo.
echo Auto-watch is running.
echo Drop files into inputs\auto_drop as exact names:
echo - latest_inventory.csv
echo - latest_sales.csv
echo.
echo Debug outputs:
echo - outputs\auto_watch.log
echo - outputs\auto_watch_status.json
echo.
echo Press Ctrl+C to stop.
%PY_CMD% src\watch_inventory_sales.py --watch --interval 20 --strict-latest-names

echo.
echo Auto-watch stopped.
pause
