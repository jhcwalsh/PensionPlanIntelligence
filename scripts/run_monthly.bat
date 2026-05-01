@echo off
REM ------------------------------------------------------------------------
REM Monthly local cadence — 1st of month. CAFR refresh + extraction + the
REM monthly CIO Insights synthesis. Pushes the DB at the end.
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

echo [%TIME%] extract_cafr_investments.py >> "%LOG%"
python extract_cafr_investments.py >> "%LOG%" 2>&1
if errorlevel 1 (
    python -m scripts.notify_failure %TASK% extract_cafr "%LOG%" %ERRORLEVEL%
    exit /b 1
)

echo [%TIME%] insights.scheduler monthly >> "%LOG%"
python -m insights.scheduler monthly >> "%LOG%" 2>&1
if errorlevel 1 (
    python -m scripts.notify_failure %TASK% insights_monthly "%LOG%" %ERRORLEVEL%
    exit /b 1
)

git add db/pension.db notes/ cafr_summaries/ >> "%LOG%" 2>&1
git diff-index --quiet HEAD
if errorlevel 1 (
    git commit -m "Monthly refresh %DATE%" >> "%LOG%" 2>&1
    git push origin master >> "%LOG%" 2>&1
    if errorlevel 1 (
        python -m scripts.notify_failure %TASK% git_push "%LOG%" %ERRORLEVEL%
        exit /b 1
    )
    echo [%TIME%] pushed monthly refresh >> "%LOG%"
) else (
    echo [%TIME%] no changes to push >> "%LOG%"
)

echo === [%DATE% %TIME%] %TASK% completed === >> "%LOG%"
endlocal
exit /b 0
