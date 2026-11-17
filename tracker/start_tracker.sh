#!/usr/bin/env bash
# Crypto Tracker - macOS / Linux starter.
# Serves the report on port 8000 and refreshes every 2 minutes.
# Restarts by itself after 30 s if it crashes. Ctrl+C to quit.
cd "$(dirname "$0")" || exit 1
trap 'echo; echo "Stopped."; exit 0' INT TERM

if [ ! -d .venv ]; then
  echo "First run: creating a private Python environment..."
  python3 -m venv .venv || { echo "Install Python 3 first."; exit 1; }
fi
echo "Installing / checking requirements..."
.venv/bin/python -m pip install -q -r requirements.txt

# On a Mac, keep the computer awake while the tracker runs.
KEEP_AWAKE=""
command -v caffeinate >/dev/null 2>&1 && KEEP_AWAKE="caffeinate -is"

while true; do
  $KEEP_AWAKE .venv/bin/python crypto_tracker.py --serve 8000 --watch 2 --quiet
  echo "Tracker stopped. Restarting in 30 seconds (Ctrl+C to quit)..."
  sleep 30
done
