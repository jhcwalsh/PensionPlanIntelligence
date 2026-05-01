@echo off
REM ------------------------------------------------------------------------
REM Quarterly local cadence — 1st of Jan/Apr/Jul/Oct at 09:00.
REM Composes a quarter-in-review CIO Insight, emails approval link,
REM pushes DB.
REM
REM Note: insights.scheduler still has only weekly/monthly/annual sub-
REM commands. We invoke 'annual' here as the closest existing entry
REM point — it'll compose from whatever monthlies are approved in the
REM lookback window. The prompt logic in insights/annual.py is tuned
REM for a 12-month window; tweaking it for a 3-month quarterly window
REM is a small follow-up but not blocking.
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
