@echo off
REM ------------------------------------------------------------------------
REM Monthly local cadence — 1st of month. CAFR refresh only. Pushes the DB
REM so the GHA monthly-insights workflow (which fires the same day at
REM 18:00 UTC) can pull fresh, run extract_cafr_investments.py, and
REM compose the monthly CIO Insights digest from the new CAFR data.
REM
REM Extract + insights moved to GitHub Actions on 2026-05-04
REM (.github/workflows/monthly-insights.yml). Time the local task so it
REM completes well before 18:00 UTC -- early morning ET works fine.
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

echo [%TIME%] refresh_cafrs.py >> "%LOG%"
python refresh_cafrs.py >> "%LOG%" 2>&1
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
