#!/usr/bin/env bash
# Start Parquet Studio locally. Creates the virtualenv on first run.
set -euo pipefail
cd "$(dirname "$0")"

PORT="${PORT:-8000}"
HOST="${HOST:-127.0.0.1}"

if [ ! -d .venv ]; then
  echo "Creating virtualenv…"
  python3 -m venv .venv
  .venv/bin/pip install --quiet --upgrade pip
  .venv/bin/pip install --quiet -r requirements.txt
fi

URL="http://${HOST}:${PORT}"
echo "Parquet Studio → ${URL}"
( sleep 1.5; command -v open >/dev/null && open "${URL}" ) &

exec .venv/bin/uvicorn app.main:app --host "${HOST}" --port "${PORT}" "$@"
