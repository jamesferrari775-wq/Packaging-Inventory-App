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
  echo Install Python, then run this launcher again.
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
echo Starting server in a new window...
start "Priority Pipeline Local UI" cmd /k "%PY_CMD% web\app.py --host 127.0.0.1 --port 5050"

echo Waiting for server startup...
timeout /t 3 /nobreak >nul

echo Opening local landing page at http://127.0.0.1:5050/
start "" "http://127.0.0.1:5050/"

echo.
echo If the page does not load, wait 2-3 seconds and refresh once.
pause
