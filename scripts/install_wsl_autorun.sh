#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SERVICE_NAME="gpgetter-daily"
DAILY_TIME="${GPGETTER_DAILY_TIME:-18:30}"
RUN_NOW=0

usage() {
  cat <<USAGE
用法:
  bash scripts/install_wsl_autorun.sh [--run-now]

环境变量:
  GPGETTER_DAILY_TIME=18:30  每日运行时间，24 小时制，默认 18:30

说明:
  这个脚本会在 WSL2 Ubuntu 里安装 systemd 定时器。
  WSL 没有启动时，Ubuntu 内部定时器不会运行；如果需要 Windows 级别唤醒，
  再运行 scripts/install_windows_daily_task.ps1。
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

if [[ ! -d /run/systemd/system ]]; then
  cat >&2 <<'EOF'
当前 WSL Ubuntu 没有启用 systemd，无法安装启动定时器。

处理方式:
1. 在 /etc/wsl.conf 中加入:
   [boot]
   systemd=true
2. 在 Windows PowerShell 执行:
   wsl --shutdown
3. 重新进入 Ubuntu 后，再运行本脚本。
EOF
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "未找到 python3，请先安装 Python 3。" >&2
  exit 1
fi

chmod +x "$PROJECT_DIR/scripts/run_daily.sh"

SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
TIMER_FILE="/etc/systemd/system/${SERVICE_NAME}.timer"
CURRENT_USER="$(id -un)"
CURRENT_GROUP="$(id -gn)"

sudo tee "$SERVICE_FILE" >/dev/null <<SERVICE
[Unit]
Description=GPgetter daily stock screener
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
User=${CURRENT_USER}
Group=${CURRENT_GROUP}
WorkingDirectory=${PROJECT_DIR}
Environment=PYTHONUNBUFFERED=1
ExecStart=${PROJECT_DIR}/scripts/run_daily.sh
SERVICE

sudo tee "$TIMER_FILE" >/dev/null <<TIMER
[Unit]
Description=Run GPgetter stock screener every day

[Timer]
OnCalendar=*-*-* ${DAILY_TIME}:00
Persistent=true
RandomizedDelaySec=5m
Unit=${SERVICE_NAME}.service

[Install]
WantedBy=timers.target
TIMER

sudo systemctl daemon-reload
sudo systemctl enable --now "${SERVICE_NAME}.timer"

if [[ "$RUN_NOW" -eq 1 ]]; then
  sudo systemctl start "${SERVICE_NAME}.service"
fi

echo "已安装 WSL systemd 定时器: ${SERVICE_NAME}.timer"
echo "每日运行时间: ${DAILY_TIME}"
echo "查看状态: systemctl status ${SERVICE_NAME}.timer"
echo "查看日志: journalctl -u ${SERVICE_NAME}.service -n 80 --no-pager"
echo "网页输出: ${PROJECT_DIR}/机构涨停候选股.html"
