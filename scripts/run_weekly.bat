@echo off
REM ------------------------------------------------------------------------
REM Weekly local cadence — Sunday morning. Composes the CIO Insights weekly
REM digest, emails an approve/reject magic-link to APPROVAL_EMAIL_RECIPIENT,
REM and AUTO-PUSHES the DB so the production Streamlit can resolve the
REM token when you click. (Pattern A1 from the deployment plan.)
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

echo [%TIME%] insights.scheduler weekly --skip-scrape >> "%LOG%"
python -m insights.scheduler weekly --skip-scrape >> "%LOG%" 2>&1
if errorlevel 1 (
    python -m scripts.notify_failure %TASK% insights_weekly "%LOG%" %ERRORLEVEL%
    exit /b 1
)

REM A1: push the DB so production Streamlit sees the pending publication
REM and the token in the email actually resolves.
git add db/pension.db notes/ >> "%LOG%" 2>&1
git diff-index --quiet HEAD
if errorlevel 1 (
    git commit -m "Weekly insights cycle %DATE%" >> "%LOG%" 2>&1
    git push origin master >> "%LOG%" 2>&1
    if errorlevel 1 (
        python -m scripts.notify_failure %TASK% git_push "%LOG%" %ERRORLEVEL%
        exit /b 1
    )
    echo [%TIME%] pushed weekly cycle >> "%LOG%"
) else (
    echo [%TIME%] no changes to push (cycle was idempotent) >> "%LOG%"
)

echo === [%DATE% %TIME%] %TASK% completed === >> "%LOG%"
endlocal
exit /b 0
