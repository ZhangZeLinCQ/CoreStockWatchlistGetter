# GPgetter

筛选沪深主板非 ST 股票，按机构持仓家数和近一年涨停次数生成候选股列表，并自动对比上一份同口径快照生成变化网页。

## 手动运行

```bash
cd /mnt/d/GitProject/GPgetter
python3 stock_screener.py
```

默认输出:

- `机构涨停候选股.html`: 最新主页，包含全部候选股、近 5 日新增股票、近 5 日消失股票
- `机构涨停候选股变化.html`: 原有变化分析网页，继续保留
- `output/details/股票代码.html`: 当前候选股详情页，展示近 30 日资金量、机构数、涨停次数趋势图
- `output/screened_stocks_日期_d365_lu5_inst30.csv`: 每日候选股归档
- `output/screened_stocks_日期_d365_lu5_inst30.html`: 每日候选股网页归档
- `output/analysis_日期_d365_lu5_inst30.csv`: 每日变化归档
- `output/analysis_日期_d365_lu5_inst30.html`: 每日变化网页归档

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

## 日志

自动运行脚本会把日志写到:

```text
logs/gpgetter_YYYYMMDD.log
```
