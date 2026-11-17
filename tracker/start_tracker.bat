@echo off
REM Crypto Tracker - Windows starter. Double-click to run.
REM Serves the report on port 8000 and refreshes every 2 minutes.
REM If it ever crashes it restarts by itself after 30 seconds.
title Crypto Tracker (keep this window open)
cd /d "%~dp0"

where py >nul 2>nul && (set PY=py) || (set PY=python)

echo Installing / checking requirements...
%PY% -m pip install -q -r requirements.txt

:loop
%PY% crypto_tracker.py --serve 8000 --watch 2 --quiet
echo.
echo Tracker stopped. Restarting in 30 seconds (close this window to quit)...
timeout /t 30 >nul
goto loop
