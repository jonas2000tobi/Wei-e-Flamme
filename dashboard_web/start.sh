#!/usr/bin/env bash
set -e
# Chromium für Playwright installieren. Wenn Railway den Browser bereits gecached hat,
# ist das schnell. Wenn die Installation scheitert, startet das Dashboard trotzdem und
# /api/game-status-live zeigt den Fehler sauber an.
python -m playwright install chromium >/tmp/ebo-playwright-install.log 2>&1 || true
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
