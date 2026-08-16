#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPOSITORY_DIR="$(cd "$PROJECT_DIR/.." && pwd)"

docker compose --env-file "$PROJECT_DIR/.env" -f "$PROJECT_DIR/docker-compose.yml" up -d
"$REPOSITORY_DIR/venv/bin/python" "$PROJECT_DIR/scripts/bootstrap.py"
