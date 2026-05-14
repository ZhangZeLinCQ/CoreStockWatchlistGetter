# GPgetter

筛选沪深主板非 ST 股票，按机构持仓家数和近一年涨停次数生成候选股列表，并自动对比上一份同口径快照生成变化网页。

## 手动运行

```bash
cd /path/to/GPgetter
python3 src/gpgetter/stock_screener.py
```

默认输出:

- `output/latest/index.html`: 最新主页，包含全部候选股、近 5 日新增股票、近 5 日消失股票
- `output/latest/watchlist.html`: 最新自选页，可从主页勾选股票后集中查看自选表和近 30 日多股票趋势图
- `output/latest/changes.html`: 最新变化分析网页
- `output/latest/candidates.md`: 最新候选股 Markdown 摘要
- `output/latest/changes.md`: 最新变化分析 Markdown 摘要
- `output/details/股票代码.html`: 当前候选股详情页，展示近 30 日资金量、机构数、涨停次数趋势图
- `output/screened_stocks_日期_d365_lu5_inst30.csv`: 每日候选股归档
- `output/screened_stocks_日期_d365_lu5_inst30.html`: 每日候选股网页归档
- `output/analysis_日期_d365_lu5_inst30.csv`: 每日变化归档
- `output/analysis_日期_d365_lu5_inst30.html`: 每日变化网页归档

## 回填过去 30 日历史数据

如果要一次性补齐截至今天的过去 30 个自然日快照:

```bash
python3 src/gpgetter/stock_screener.py --backfill-days 30
```

如需指定截止日期，例如补齐截至 2026-05-10 的 30 天:

```bash
python3 src/gpgetter/stock_screener.py --backfill-days 30 --end-date 20260510
```

回填会按日期从旧到新生成每日归档，文件名仍使用现有口径:

- `output/screened_stocks_YYYYMMDD_d365_lu5_inst30.csv`
- `output/screened_stocks_YYYYMMDD_d365_lu5_inst30.html`
- `output/analysis_YYYYMMDD_d365_lu5_inst30.csv`
- `output/analysis_YYYYMMDD_d365_lu5_inst30.html`

如果某一天同口径结果已经存在，默认会跳过重复拉取；需要覆盖时加上 `--force-update`。
详情页的近 30 日趋势图会读取这些每日快照生成。
回填模式下，历史资金量会按对应交易日的日线成交量和收盘价估算，避免写入运行当天的实时资金量。
机构数仍来自东方财富当前可用的最新机构持仓报告期，不按历史披露时点回放。

## 目录结构

- `src/gpgetter/`: 业务代码
- `scripts/`: 日常执行和定时安装脚本
- `output/`: 运行产物与网页归档，默认不纳入版本管理
- `logs/`: 自动任务日志，默认不纳入版本管理

## WSL2 Ubuntu 自动运行

先确认 WSL2 Ubuntu 已启用 systemd。然后执行:

```bash
cd /mnt/d/GitProject/GPgetter
bash scripts/install_wsl_autorun.sh
```

默认每天 18:30 运行。修改时间:

```bash
GPGETTER_DAILY_TIME=19:00 bash scripts/install_wsl_autorun.sh
```

安装后常用查看命令:

```bash
systemctl status gpgetter-daily.timer
journalctl -u gpgetter-daily.service -n 80 --no-pager
```

如果希望安装后立刻跑一次:

```bash
bash scripts/install_wsl_autorun.sh --run-now
```

## Windows 计划任务

如果希望 Windows 即使没有手动打开 Ubuntu，也能在固定时间启动 WSL 并执行更新，可以在 Windows PowerShell 运行:

```powershell
powershell -ExecutionPolicy Bypass -File D:\GitProject\GPgetter\scripts\install_windows_daily_task.ps1 -WslDistro Ubuntu -DailyTime 18:30
```

如你的发行版名称不是 `Ubuntu`，先在 PowerShell 执行 `wsl -l -q` 查看名称，再替换 `-WslDistro`。

## macOS 自动运行

macOS 可直接复用同一套每日执行脚本:

```bash
cd /path/to/GPgetter
bash scripts/install_macos_launchagent.sh
```

默认每天 18:30 运行。修改时间:

```bash
GPGETTER_DAILY_TIME=19:00 bash scripts/install_macos_launchagent.sh
```

如果希望安装后立刻跑一次:

```bash
bash scripts/install_macos_launchagent.sh --run-now
```

该脚本会创建当前用户的 `LaunchAgent`，后续仍由 `scripts/run_daily.sh` 负责建虚拟环境、安装依赖和生成结果。

## 日志

自动运行脚本会把日志写到:

```text
logs/gpgetter_YYYYMMDD.log
```
