# Register the one remaining scheduled task in Windows Task Scheduler.
#
#   powershell -ExecutionPolicy Bypass -File scripts\register_tasks.ps1
#
# As of 2026-08-16 everything except the meeting-recordings catalogue runs on
# GitHub Actions, so no machine of James's is in the path of the app working.
# The recordings job is a deliberate exception: it is a side dataset, and if
# this machine is off for a month nothing else degrades.
#
# Edits any existing PensionPipeline-* task rather than duplicating. Runs as
# the current user (so the venv + git creds are available). "Run task as soon
# as possible after a missed start" is enabled so a laptop closed at trigger
# time still catches up when it wakes.

$ErrorActionPreference = "Stop"
$Repo = "C:\Users\james\PycharmProjects\PensionPlanIntelligence"
$User = "$env:USERDOMAIN\$env:USERNAME"

# Unregister tasks that have moved to GitHub Actions. Leaving these in place
# would keep running deleted .bat files and pushing to a branch that no longer
# expects local writers.
foreach ($legacy in @(
    "PensionPipeline-Annual",
    "PensionPipeline-Weekly",       # -> weekly-rfp.yml (2026-05-04)
    "PensionPipeline-Quarterly",    # -> quarterly-insights.yml (2026-05-04)
    "PensionPipeline-Daily",        # -> daily-pipeline.yml (2026-08-16)
    "PensionPipeline-Monthly",      # -> monthly-cafr-refresh.yml (2026-08-16)
    "PensionPipeline-IPS"           # -> monthly-ips.yml (2026-08-16)
)) {
    if (Get-ScheduledTask -TaskName $legacy -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $legacy -Confirm:$false
        Write-Host "Removed task $legacy (now runs on GitHub Actions)"
    }
}

# Recordings — Saturdays 08:00 local. Catalogue-only (--no-downloads):
# discovers video sources, polls them for new meeting recordings, emails
# the digest, pushes DB metadata. Saturday so the catalogue is fresh
# before the Sunday GHA insights/RFP runs. Downloads stay manual — see
# scripts/run_recordings.bat.
$RecAction = New-ScheduledTaskAction `
    -Execute "$Repo\scripts\run_recordings.bat" `
    -Argument "--no-downloads" `
    -WorkingDirectory $Repo
$RecSettings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -DontStopIfGoingOnBatteries `
    -AllowStartIfOnBatteries `
    -RunOnlyIfNetworkAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 6)
$RecPrincipal = New-ScheduledTaskPrincipal `
    -UserId $User `
    -LogonType Interactive `
    -RunLevel Limited
Register-ScheduledTask `
    -TaskName "PensionPipeline-Recordings" `
    -Action $RecAction `
    -Trigger (New-ScheduledTaskTrigger -Weekly -DaysOfWeek Saturday -At 8:00am) `
    -Settings $RecSettings `
    -Principal $RecPrincipal `
    -Force
Write-Host "Registered PensionPipeline-Recordings"

# Weekly + Quarterly tasks moved to GitHub Actions on 2026-05-04.
# See .github/workflows/weekly-rfp.yml and quarterly-insights.yml.
# The legacy-cleanup loop at the top of this script unregisters those
# tasks on next re-run.

Write-Host ""
Write-Host "Task registered. Verify with:"
Write-Host "    Get-ScheduledTask -TaskName 'PensionPipeline-*'"
Write-Host ""
Write-Host "Run one manually to test (won't wait for the trigger):"
Write-Host "    Start-ScheduledTask -TaskName 'PensionPipeline-Recordings'"
