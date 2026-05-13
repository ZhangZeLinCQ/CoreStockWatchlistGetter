#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DAILY_TIME="${GPGETTER_DAILY_TIME:-18:30}"
LABEL="com.gpgetter.daily"
PLIST_DIR="$HOME/Library/LaunchAgents"
PLIST_FILE="$PLIST_DIR/${LABEL}.plist"
RUN_NOW=0

usage() {
  cat <<USAGE
用法:
  bash scripts/install_macos_launchagent.sh [--run-now]

环境变量:
  GPGETTER_DAILY_TIME=18:30  每日运行时间，24 小时制，默认 18:30
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-now)
      RUN_NOW=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "未知参数: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! "$DAILY_TIME" =~ ^([01]?[0-9]|2[0-3]):[0-5][0-9]$ ]]; then
  echo "GPGETTER_DAILY_TIME 必须是 HH:MM，例如 18:30" >&2
  exit 2
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "未找到 python3，请先安装 Python 3。" >&2
  exit 1
fi

chmod +x "$PROJECT_DIR/scripts/run_daily.sh"
mkdir -p "$PLIST_DIR"

HOUR="${DAILY_TIME%:*}"
MINUTE="${DAILY_TIME#*:}"

cat > "$PLIST_FILE" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${PROJECT_DIR}/scripts/run_daily.sh</string>
  </array>
  <key>WorkingDirectory</key>
  <string>${PROJECT_DIR}</string>
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key>
    <integer>${HOUR}</integer>
    <key>Minute</key>
    <integer>${MINUTE}</integer>
  </dict>
  <key>RunAtLoad</key>
  <false/>
</dict>
</plist>
PLIST

launchctl bootout "gui/$(id -u)" "$PLIST_FILE" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_FILE"

if [[ "$RUN_NOW" -eq 1 ]]; then
  launchctl kickstart -k "gui/$(id -u)/${LABEL}"
fi

echo "已安装 macOS LaunchAgent: ${LABEL}"
echo "每日运行时间: ${DAILY_TIME}"
echo "任务文件: ${PLIST_FILE}"
echo "网页输出: ${PROJECT_DIR}/机构涨停候选股.html"
