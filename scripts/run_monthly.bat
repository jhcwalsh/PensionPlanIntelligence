@echo off
REM ------------------------------------------------------------------------
REM Monthly local cadence — 1st of month. CAFR refresh, scoped to the 5
REM WAF-blocked plans only (data/local_only_cafr_plans.json). The other
REM ~92 CAFR-having plans run on .github/workflows/monthly-cafr-refresh.yml
REM at 15:00 UTC the same day; this local task completes well before
REM monthly-insights (18:00 UTC) pulls fresh.
REM
REM CAFR migration to GHA landed 2026-05-05 (probe SHA 4bd08c0). Extract
REM + insights moved earlier on 2026-05-04
REM (.github/workflows/monthly-insights.yml).
REM ------------------------------------------------------------------------

setlocal
cd /d C:\Users\james\PycharmProjects\PensionPlanIntelligence
if not exist logs mkdir logs
set TASK=monthly
set LOG=logs\%TASK%.log

echo. >> "%LOG%"
echo === [%DATE% %TIME%] Starting %TASK% === >> "%LOG%"

call .venv\Scripts\activate.bat
if errorlevel 1 (
    .venv\Scripts\python.exe -m scripts.notify_failure %TASK% venv_activate "%LOG%" 1
    exit /b 1
)

echo [%TIME%] refresh_cafrs.py --local-only >> "%LOG%"
python refresh_cafrs.py --local-only >> "%LOG%" 2>&1
if errorlevel 1 (
    python -m scripts.notify_failure %TASK% refresh_cafrs "%LOG%" %ERRORLEVEL%
    exit /b 1
)

git add db/pension.db >> "%LOG%" 2>&1
git diff-index --quiet HEAD
if errorlevel 1 (
    git commit -m "Monthly CAFR refresh %DATE%" >> "%LOG%" 2>&1
    git push origin master >> "%LOG%" 2>&1
    if errorlevel 1 (
        python -m scripts.notify_failure %TASK% git_push "%LOG%" %ERRORLEVEL%
        exit /b 1
    )
    echo [%TIME%] pushed monthly CAFR refresh >> "%LOG%"
) else (
    echo [%TIME%] no changes to push >> "%LOG%"
)

echo === [%DATE% %TIME%] %TASK% completed === >> "%LOG%"
endlocal
exit /b 0
