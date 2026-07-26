#!/usr/bin/env bash
set -euo pipefail

# Chromium wird beim Image-Build installiert (siehe Dockerfile), nicht bei jedem Start.
exec uvicorn main:app --host 0.0.0.0 --port "${PORT:-8000}"
