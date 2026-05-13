#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$PROJECT_DIR/logs"
VENV_DIR="$PROJECT_DIR/.venv"
PYTHON_BIN="$VENV_DIR/bin/python"
REQUIREMENTS_STAMP="$VENV_DIR/.requirements.stamp"
INSTALL_DEPS=0

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/gpgetter_$(date +%Y%m%d).log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "[$(date '+%Y-%m-%d %H:%M:%S')] GPgetter daily update started"
cd "$PROJECT_DIR"

if [[ ! -x "$PYTHON_BIN" ]]; then
  python3 -m venv "$VENV_DIR"
  INSTALL_DEPS=1
fi

if [[ ! -f "$REQUIREMENTS_STAMP" || "$PROJECT_DIR/requirements.txt" -nt "$REQUIREMENTS_STAMP" ]]; then
  INSTALL_DEPS=1
fi

if [[ "$INSTALL_DEPS" -eq 1 ]]; then
  "$PYTHON_BIN" -m pip install -r "$PROJECT_DIR/requirements.txt"
  touch "$REQUIREMENTS_STAMP"
fi

"$PYTHON_BIN" "$PROJECT_DIR/src/gpgetter/stock_screener.py" "$@"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] GPgetter daily update finished"
echo "日志文件: $LOG_FILE"
