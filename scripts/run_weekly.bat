@echo off
REM ------------------------------------------------------------------------
REM DEPRECATED FOR SCHEDULED USE — moved to GitHub Actions on 2026-05-04
REM (.github/workflows/weekly-rfp.yml fires Sundays at 11:30 UTC).
REM
REM Kept in the repo as a manual fallback only — if GHA is unavailable,
REM run this directly:  scripts\run_weekly.bat
REM Otherwise the Windows Task Scheduler entry has been removed; see
REM scripts\register_tasks.ps1.
REM
REM Original purpose: bounded RFP backfill (--limit 100) + push.
REM ------------------------------------------------------------------------

setlocal
cd /d C:\Users\james\PycharmProjects\PensionPlanIntelligence
if not exist logs mkdir logs
set TASK=weekly
set LOG=logs\%TASK%.log

echo. >> "%LOG%"
echo === [%DATE% %TIME%] Starting %TASK% === >> "%LOG%"

call .venv\Scripts\activate.bat
if errorlevel 1 (
    .venv\Scripts\python.exe -m scripts.notify_failure %TASK% venv_activate "%LOG%" 1
    exit /b 1
)

REM Bounded RFP backfill: 100 docs per Sunday ~ $3/week worst case.
REM Idempotent on (document_id, prompt_version) so the backfill
REM completes naturally over many runs once document_health is reset.
echo [%TIME%] run_rfp_extraction --limit 100 >> "%LOG%"
python -m scripts.run_rfp_extraction --limit 100 >> "%LOG%" 2>&1
if errorlevel 1 (
    python -m scripts.notify_failure %TASK% rfp_extraction "%LOG%" %ERRORLEVEL%
    exit /b 1
)

git add db/pension.db >> "%LOG%" 2>&1
git diff-index --quiet HEAD
if errorlevel 1 (
    git commit -m "Weekly RFP backfill %DATE%" >> "%LOG%" 2>&1
    git push origin master >> "%LOG%" 2>&1
    if errorlevel 1 (
        python -m scripts.notify_failure %TASK% git_push "%LOG%" %ERRORLEVEL%
        exit /b 1
    )
    echo [%TIME%] pushed weekly RFP backfill >> "%LOG%"
) else (
    echo [%TIME%] no changes to push >> "%LOG%"
)

echo === [%DATE% %TIME%] %TASK% completed === >> "%LOG%"
endlocal
exit /b 0
