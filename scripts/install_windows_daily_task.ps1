[CmdletBinding()]
param(
    [string]$TaskName = "GPgetter Daily Update",
    [string]$WslDistro = "",
    [string]$ProjectDir = "/mnt/d/GitProject/GPgetter",
    [string]$DailyTime = "18:30"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command wsl.exe -ErrorAction SilentlyContinue)) {
    throw "未找到 wsl.exe，请确认 WSL 已安装。"
}

if ([string]::IsNullOrWhiteSpace($WslDistro)) {
    $names = & wsl.exe -l -q |
        ForEach-Object { ($_ -replace "`0", "").Trim() } |
        Where-Object { $_ }
    if (-not $names -or $names.Count -eq 0) {
        throw "未找到 WSL 发行版，请用 -WslDistro 指定，例如 Ubuntu。"
    }
    $WslDistro = $names[0]
}

$runAt = [datetime]::ParseExact(
    $DailyTime,
    "HH:mm",
    [System.Globalization.CultureInfo]::InvariantCulture
)

$arguments = "-d `"$WslDistro`" --cd `"$ProjectDir`" --exec bash scripts/run_daily.sh"
$action = New-ScheduledTaskAction -Execute "$env:WINDIR\System32\wsl.exe" -Argument $arguments
$trigger = New-ScheduledTaskTrigger -Daily -At $runAt
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "每天启动 WSL2 Ubuntu 并运行 GPgetter 更新 HTML 变化网页。" `
    -Force | Out-Null

Write-Host "已创建 Windows 计划任务: $TaskName"
Write-Host "WSL 发行版: $WslDistro"
Write-Host "每日运行时间: $DailyTime"
Write-Host "运行命令: wsl.exe $arguments"
