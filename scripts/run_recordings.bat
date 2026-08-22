@echo off
REM ------------------------------------------------------------------------
REM Meeting recordings catalogue refresh / notify — local Windows only.
REM
REM Sequential steps:
REM   1. discover_video_sources.py — mine newly-extracted documents for
REM                                  video archive/channel URLs (offline)
REM   2. refresh_recordings.py     — poll active video sources for new vids
REM   3. download_recordings.py    — fetch new pending rows to D:\
REM                                  (skipped by the scheduled task; the
REM                                  catalogue is the product, a board video
REM                                  is 1-3 GB — download manually on demand)
REM   4. notify_new_recordings.py  — email digest of newly-discovered videos
REM
REM Recordings live on the local D: drive (D:\PensionGraph\meetingrecordings),
REM not on Render. Only the metadata lives in the database -- now Postgres,
REM which both this job and Render read directly, so the catalogue tab is in
REM sync the moment a row is written. No commit, no push, no time slot.
REM
REM Schedule: weekly, Saturdays 08:00 local with --no-downloads (see
REM register_tasks.ps1), so the catalogue is fresh before the Sunday GHA
REM insights/RFP runs.
REM
REM Manual run:
REM   scripts\run_recordings.bat                   # all steps
REM   scripts\run_recordings.bat --no-downloads    # poll + notify only
REM ------------------------------------------------------------------------

setlocal
REM Rich console output crashes on cp1252 when redirected to a log file.
set PYTHONIOENCODING=utf-8
cd /d C:\Users\james\PycharmProjects\PensionPlanIntelligence
if not exist logs mkdir logs
set TASK=recordings
set LOG=logs\%TASK%.log
set DOWNLOAD_LIMIT=10

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

echo [%TIME%] discover_video_sources.py >> "%LOG%"
python discover_video_sources.py >> "%LOG%" 2>&1
if errorlevel 1 (
    python -m scripts.notify_failure %TASK% discover "%LOG%" %ERRORLEVEL%
    exit /b 1
)

echo [%TIME%] refresh_recordings.py >> "%LOG%"
python refresh_recordings.py >> "%LOG%" 2>&1
if errorlevel 1 (
    python -m scripts.notify_failure %TASK% refresh "%LOG%" %ERRORLEVEL%
    exit /b 1
)

if /i "%~1"=="--no-downloads" goto :notify

echo [%TIME%] download_recordings.py --limit %DOWNLOAD_LIMIT% >> "%LOG%"
python download_recordings.py --limit %DOWNLOAD_LIMIT% >> "%LOG%" 2>&1
if errorlevel 1 (
    REM Download failures are not fatal for the run — keep going so the
    REM notification still fires for whatever was discovered.
    echo [%TIME%] download step exited %ERRORLEVEL% >> "%LOG%"
)

:notify
echo [%TIME%] notify_new_recordings.py >> "%LOG%"
python notify_new_recordings.py >> "%LOG%" 2>&1
if errorlevel 1 (
    python -m scripts.notify_failure %TASK% notify "%LOG%" %ERRORLEVEL%
    exit /b 1
)

REM The catalogue rows are already in Postgres by this point -- they were
REM written as refresh_recordings.py went. Nothing to commit or push: the
REM database left git in the 2026-08-21 cutover, which is what removed this
REM job's conflict-avoidance time slot along with it.

echo === [%DATE% %TIME%] %TASK% completed === >> "%LOG%"
endlocal
exit /b 0
