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
Register-PipelineTask `
    -Name "Daily" `
    -BatFile "run_daily.bat" `
    -Trigger (New-ScheduledTaskTrigger -Daily -At 6:00am)

# Weekly — Sunday at 07:00 (after daily completes).
Register-PipelineTask `
    -Name "Weekly" `
    -BatFile "run_weekly.bat" `
    -Trigger (New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At 7:00am)

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

# Annual — Jan 5 at 09:00.
$AnnualDef = $MonthlyService.NewTask(0)
$AnnualDef.RegistrationInfo.Description = "PensionPipeline annual cron-equivalent"
$AnnualDef.Settings.StartWhenAvailable = $true
$AnnualDef.Settings.DisallowStartIfOnBatteries = $false
$AnnualDef.Settings.StopIfGoingOnBatteries = $false
$AnnualDef.Settings.RunOnlyIfNetworkAvailable = $true
$AnnualDef.Settings.ExecutionTimeLimit = "PT6H"
$AnnualTrigger = $AnnualDef.Triggers.Create(4)
$AnnualTrigger.StartBoundary = (Get-Date -Hour 9 -Minute 0 -Second 0).ToString("s")
$AnnualTrigger.DaysOfMonth = 5
$AnnualTrigger.MonthsOfYear = 1  # January only
$AnnualAction = $AnnualDef.Actions.Create(0)
$AnnualAction.Path = "$Repo\scripts\run_annual.bat"
$AnnualAction.WorkingDirectory = $Repo
$MonthlyFolder.RegisterTaskDefinition(
    "PensionPipeline-Annual", $AnnualDef, 6, $User, $null, 3
) | Out-Null
Write-Host "Registered PensionPipeline-Annual"

Write-Host ""
Write-Host "All 4 tasks registered. Verify with:"
Write-Host "    Get-ScheduledTask -TaskName 'PensionPipeline-*'"
Write-Host ""
Write-Host "Run one manually to test (won't wait for the trigger):"
Write-Host "    Start-ScheduledTask -TaskName 'PensionPipeline-Daily'"
