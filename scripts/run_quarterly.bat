@echo off
REM ------------------------------------------------------------------------
REM DEPRECATED FOR SCHEDULED USE — moved to GitHub Actions on 2026-05-04
REM (.github/workflows/quarterly-insights.yml fires 1st of Jan/Apr/Jul/Oct
REM at 19:00 UTC).
REM
REM Kept in the repo as a manual fallback only — if GHA is unavailable,
REM run this directly:  scripts\run_quarterly.bat
REM Otherwise the Windows Task Scheduler entry has been removed; see
REM scripts\register_tasks.ps1.
REM
REM Original purpose: quarter-in-review CIO Insight (uses 'annual' sub-
REM command since insights.scheduler doesn't have a 'quarterly' verb yet).
REM ------------------------------------------------------------------------

setlocal
cd /d C:\Users\james\PycharmProjects\PensionPlanIntelligence
if not exist logs mkdir logs
set TASK=quarterly
set LOG=logs\%TASK%.log

echo. >> "%LOG%"
echo === [%DATE% %TIME%] Starting %TASK% === >> "%LOG%"

call .venv\Scripts\activate.bat
if errorlevel 1 (
    .venv\Scripts\python.exe -m scripts.notify_failure %TASK% venv_activate "%LOG%" 1
    exit /b 1
)

echo [%TIME%] insights.scheduler annual (used quarterly until a quarter-aware command exists) >> "%LOG%"
python -m insights.scheduler annual >> "%LOG%" 2>&1
if errorlevel 1 (
    python -m scripts.notify_failure %TASK% insights_quarterly "%LOG%" %ERRORLEVEL%
    exit /b 1
)

git add db/pension.db notes/ >> "%LOG%" 2>&1
git diff-index --quiet HEAD
if errorlevel 1 (
    git commit -m "Quarterly insights cycle %DATE%" >> "%LOG%" 2>&1
    git push origin master >> "%LOG%" 2>&1
    if errorlevel 1 (
        python -m scripts.notify_failure %TASK% git_push "%LOG%" %ERRORLEVEL%
        exit /b 1
    )
    echo [%TIME%] pushed quarterly cycle >> "%LOG%"
) else (
    echo [%TIME%] no changes to push >> "%LOG%"
)

echo === [%DATE% %TIME%] %TASK% completed === >> "%LOG%"
endlocal
exit /b 0
