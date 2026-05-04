# Register all four scheduled tasks in Windows Task Scheduler.
# Run once, from an elevated PowerShell prompt:
#
#   powershell -ExecutionPolicy Bypass -File scripts\register_tasks.ps1
#
# Edits any existing PensionPipeline-* tasks rather than duplicating.
# Tasks run as the current user (so the venv + git creds are available).
# "Run task as soon as possible after a missed start" is enabled so a
# laptop closed at trigger time still catches up when it wakes.

$ErrorActionPreference = "Stop"
$Repo = "C:\Users\james\PycharmProjects\PensionPlanIntelligence"
$User = "$env:USERDOMAIN\$env:USERNAME"

# Clean up any task names from previous registrations (so renames take
# effect cleanly — Register-ScheduledTask -Force only overwrites by name).
# Also unregisters tasks that have moved to GitHub Actions and no longer
# need a local Task Scheduler entry.
foreach ($legacy in @(
    "PensionPipeline-Annual",
    "PensionPipeline-Weekly",       # → .github/workflows/weekly-rfp.yml (2026-05-04)
    "PensionPipeline-Quarterly"     # → .github/workflows/quarterly-insights.yml (2026-05-04)
)) {
    if (Get-ScheduledTask -TaskName $legacy -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $legacy -Confirm:$false
        Write-Host "Removed legacy task $legacy"
    }
}

function Register-PipelineTask {
    param(
        [string]$Name,
        [string]$BatFile,
        [Microsoft.Management.Infrastructure.CimInstance]$Trigger
    )

    $Action = New-ScheduledTaskAction `
        -Execute "$Repo\scripts\$BatFile" `
        -WorkingDirectory $Repo

    $Settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -DontStopIfGoingOnBatteries `
        -AllowStartIfOnBatteries `
        -RunOnlyIfNetworkAvailable `
        -ExecutionTimeLimit (New-TimeSpan -Hours 6)

    $Principal = New-ScheduledTaskPrincipal `
        -UserId $User `
        -LogonType Interactive `
        -RunLevel Limited

    Register-ScheduledTask `
        -TaskName "PensionPipeline-$Name" `
        -Action $Action `
        -Trigger $Trigger `
        -Settings $Settings `
        -Principal $Principal `
        -Force

    Write-Host "Registered PensionPipeline-$Name"
}

# Daily — every day at 06:00 local time.
# (Daily handles the 11 WAF-blocked plans; the other 137 run on
# .github/workflows/daily-pipeline.yml at 11:00 UTC.)
Register-PipelineTask `
    -Name "Daily" `
    -BatFile "run_daily.bat" `
    -Trigger (New-ScheduledTaskTrigger -Daily -At 6:00am)

# Monthly — 1st of every month at 08:00.
# (Task Scheduler's "Monthly" trigger needs cmdlet via PowerShell 6+; on
# Windows PowerShell 5.1 we use the COM API workaround below.)
$MonthlyService = New-Object -ComObject "Schedule.Service"
$MonthlyService.Connect()
$MonthlyFolder = $MonthlyService.GetFolder("\")
$MonthlyDef = $MonthlyService.NewTask(0)
$MonthlyDef.RegistrationInfo.Description = "PensionPipeline monthly cron-equivalent"
$MonthlyDef.Settings.StartWhenAvailable = $true
$MonthlyDef.Settings.DisallowStartIfOnBatteries = $false
$MonthlyDef.Settings.StopIfGoingOnBatteries = $false
$MonthlyDef.Settings.RunOnlyIfNetworkAvailable = $true
$MonthlyDef.Settings.ExecutionTimeLimit = "PT6H"
$MonthlyTrigger = $MonthlyDef.Triggers.Create(4)  # 4 = monthly
$MonthlyTrigger.StartBoundary = (Get-Date -Hour 8 -Minute 0 -Second 0).ToString("s")
$MonthlyTrigger.DaysOfMonth = 1
$MonthlyTrigger.MonthsOfYear = 4095  # all 12 months bitmask
$MonthlyAction = $MonthlyDef.Actions.Create(0)
$MonthlyAction.Path = "$Repo\scripts\run_monthly.bat"
$MonthlyAction.WorkingDirectory = $Repo
$MonthlyFolder.RegisterTaskDefinition(
    "PensionPipeline-Monthly", $MonthlyDef, 6, $User, $null, 3
) | Out-Null
Write-Host "Registered PensionPipeline-Monthly"

# Weekly + Quarterly tasks moved to GitHub Actions on 2026-05-04.
# See .github/workflows/weekly-rfp.yml and quarterly-insights.yml.
# The legacy-cleanup loop at the top of this script unregisters those
# tasks on next re-run.

Write-Host ""
Write-Host "Tasks registered. Verify with:"
Write-Host "    Get-ScheduledTask -TaskName 'PensionPipeline-*'"
Write-Host ""
Write-Host "Run one manually to test (won't wait for the trigger):"
Write-Host "    Start-ScheduledTask -TaskName 'PensionPipeline-Daily'"
