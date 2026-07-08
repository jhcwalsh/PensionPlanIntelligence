@echo off
REM ------------------------------------------------------------------------
REM Monthly IPS refresh — runs locally (no GHA equivalent).
REM
REM Auto-discovers and fetches each plan's Investment Policy Statement.
REM Uses Claude Haiku 4.5 to verify each candidate is the comprehensive
REM IPS (vs. an adjacent policy doc) before saving. Hash-deduplicates,
REM so re-runs are no-ops when nothing has changed; new versions land
REM as new ips_documents rows tied to the plan via content_hash.
REM
REM Schedule: 1st of month, after the GHA monthly-cafr-refresh and local
REM monthly Task Scheduler entries have completed (see register_tasks.ps1).
REM Pushes db/pension.db on change so Streamlit / FastAPI on Render see
REM new IPS rows.
REM
REM Manual run from a venv-activated PowerShell:
REM   python refresh_ips.py
REM   python refresh_ips.py calpers nystrs        # subset
REM   python refresh_ips.py --discover-only       # dry run
REM ------------------------------------------------------------------------

setlocal
REM Rich console output crashes on cp1252 when redirected to a log file.
set PYTHONIOENCODING=utf-8
cd /d C:\Users\james\PycharmProjects\PensionPlanIntelligence
if not exist logs mkdir logs
set TASK=ips
set LOG=logs\%TASK%.log

echo. >> "%LOG%"
echo === [%DATE% %TIME%] Starting %TASK% === >> "%LOG%"

call .venv\Scripts\activate.bat
if errorlevel 1 (
    .venv\Scripts\python.exe -m scripts.notify_failure %TASK% venv_activate "%LOG%" 1
    exit /b 1
)

REM Sync with remote so the pipeline runs against the latest DB.
echo [%TIME%] git pull --rebase >> "%LOG%"
git pull --rebase origin master >> "%LOG%" 2>&1
if errorlevel 1 (
    echo [%TIME%] pull --rebase failed, aborting rebase >> "%LOG%"
    git rebase --abort >> "%LOG%" 2>&1
    python -m scripts.notify_failure %TASK% git_pull "%LOG%" %ERRORLEVEL%
    exit /b 1
)

echo [%TIME%] db_sync pull >> "%LOG%"
python -m scripts.db_sync pull >> "%LOG%" 2>&1
if errorlevel 1 (
    python -m scripts.notify_failure %TASK% db_sync_pull "%LOG%" %ERRORLEVEL%
    exit /b 1
)

echo [%TIME%] refresh_ips.py >> "%LOG%"
python refresh_ips.py >> "%LOG%" 2>&1
if errorlevel 1 (
    python -m scripts.notify_failure %TASK% refresh_ips "%LOG%" %ERRORLEVEL%
    exit /b 1
)

REM Stage and commit only if something actually changed.
echo [%TIME%] db_sync push >> "%LOG%"
python -m scripts.db_sync push --by %TASK% >> "%LOG%" 2>&1
if errorlevel 1 (
    python -m scripts.notify_failure %TASK% db_sync_push "%LOG%" %ERRORLEVEL%
    exit /b 1
)

git add db/pension.db >> "%LOG%" 2>&1
git diff-index --quiet HEAD
if errorlevel 1 (
    git commit -m "IPS refresh %DATE%" >> "%LOG%" 2>&1
    git push origin master >> "%LOG%" 2>&1
    if errorlevel 1 (
        echo [%TIME%] push rejected, retrying with pull --rebase >> "%LOG%"
        git pull --rebase origin master >> "%LOG%" 2>&1
        if errorlevel 1 (
            git rebase --abort >> "%LOG%" 2>&1
            python -m scripts.notify_failure %TASK% git_rebase "%LOG%" %ERRORLEVEL%
            exit /b 1
        )
        git push origin master >> "%LOG%" 2>&1
        if errorlevel 1 (
            python -m scripts.notify_failure %TASK% git_push "%LOG%" %ERRORLEVEL%
            exit /b 1
        )
    )
    echo [%TIME%] pushed IPS refresh >> "%LOG%"
) else (
    echo [%TIME%] no changes to push >> "%LOG%"
)

echo === [%DATE% %TIME%] %TASK% completed === >> "%LOG%"
endlocal
exit /b 0
