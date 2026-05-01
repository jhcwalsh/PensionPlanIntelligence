@echo off
REM ------------------------------------------------------------------------
REM Daily local cadence — fetch new docs, extract RFPs, run insights reminders.
REM Auto-commits db/pension.db and pushes if anything changed.
REM Sends a failure email to APPROVAL_EMAIL_RECIPIENT on any non-zero exit.
REM Triggered by Windows Task Scheduler — see scripts\register_tasks.ps1.
REM ------------------------------------------------------------------------

setlocal
cd /d C:\Users\james\PycharmProjects\PensionPlanIntelligence
if not exist logs mkdir logs
set TASK=daily
set LOG=logs\%TASK%.log

echo. >> "%LOG%"
echo === [%DATE% %TIME%] Starting %TASK% === >> "%LOG%"

call .venv\Scripts\activate.bat
if errorlevel 1 (
    .venv\Scripts\python.exe -m scripts.notify_failure %TASK% venv_activate "%LOG%" 1
    exit /b 1
)

echo [%TIME%] pipeline.py >> "%LOG%"
python pipeline.py >> "%LOG%" 2>&1
if errorlevel 1 (
    python -m scripts.notify_failure %TASK% pipeline "%LOG%" %ERRORLEVEL%
    exit /b 1
)

REM RFP extraction intentionally not in the daily cron until you're ready
REM to start the live LLM backfill. To kick it off:
REM   python -m scripts.run_rfp_extraction --limit 50
REM Then add this back into the daily once steady-state. Idempotent on
REM (document_id, prompt_version) so daily runs do nothing once caught up.

echo [%TIME%] insights.scheduler reminders >> "%LOG%"
python -m insights.scheduler reminders >> "%LOG%" 2>&1
if errorlevel 1 (
    python -m scripts.notify_failure %TASK% insights_reminders "%LOG%" %ERRORLEVEL%
    exit /b 1
)

REM Stage and commit only if something actually changed.
git add db/pension.db notes/ >> "%LOG%" 2>&1
git diff-index --quiet HEAD
if errorlevel 1 (
    git commit -m "Daily refresh %DATE%" >> "%LOG%" 2>&1
    git push origin master >> "%LOG%" 2>&1
    if errorlevel 1 (
        python -m scripts.notify_failure %TASK% git_push "%LOG%" %ERRORLEVEL%
        exit /b 1
    )
    echo [%TIME%] pushed daily refresh >> "%LOG%"
) else (
    echo [%TIME%] no changes to push >> "%LOG%"
)

echo === [%DATE% %TIME%] %TASK% completed === >> "%LOG%"
endlocal
exit /b 0
