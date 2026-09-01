#!/bin/bash
# ------------------------------------------------------------------------
# WAF-blocked plans -- Mac Mini only.
#
# Some plans in data/waf_blocked_plans.json and data/waf_blocked_cafr_plans.json
# sit behind bot mitigation that refuses datacentre IPs, so GitHub Actions
# subtracts them from every run. This machine has a residential IP and fetches
# exactly those plans, naming them explicitly on the CLI -- the documented
# bypass in both _resolve_plan_ids docstrings.
#
# NOT all of the block lists: only the entries marked blocked_by=datacentre_ip,
# which scripts/waf_blocked_ids.py selects. The 2026-09-01 probe found that
# rendering a listing page and downloading the PDFs it links to are independent
# problems -- four plans list perfectly and 403 on every download, and sending
# this job after them would fail nightly for a reason no host can fix. Never
# hardcode the ids; tests/test_run_waf_plans_script.py enforces that.
#
# Best-effort by design (spec 2026-08-30 §4). Rows go straight to Neon, which
# Render reads, so there is nothing to commit and nothing to deploy. If this
# machine is off for a month, coverage stops advancing on those plans and
# nothing else degrades.
#
# Manual run:  ~/apps/pensiongraph/scripts/run_waf_plans.sh
# Scheduled:   ~/Library/LaunchAgents/com.pensiongraph.wafplans.plist
# ------------------------------------------------------------------------
set -u

APP_DIR="$HOME/apps/pensiongraph"
COMPOSE="$APP_DIR/docker-compose.pipeline.yml"
TASK="waf_plans"
LOG="$APP_DIR/logs/$TASK.log"
LOCK="$APP_DIR/logs/$TASK.lock"

# Absolute paths: launchd and non-interactive SSH do not source .zprofile, so
# a bare `docker` or `git` fails with "command not found". The runbook records
# this biting scripted deploys; it bites scheduled jobs the same way.
# /usr/local/bin/docker is OrbStack's system-wide symlink, present regardless
# of whether ~/.orbstack/bin is on PATH.
GIT=/usr/bin/git
DOCKER=/usr/local/bin/docker

mkdir -p "$APP_DIR/logs"

# mkdir is atomic, and unlike flock it exists on stock macOS. launchd will not
# start a second copy of a running job anyway; this covers a manual run
# overlapping the scheduled one.
if ! mkdir "$LOCK" 2>/dev/null; then
    echo "[$(date -u +%FT%TZ)] $TASK already running, exiting" >> "$LOG"
    exit 0
fi
trap 'rmdir "$LOCK" 2>/dev/null' EXIT

cd "$APP_DIR" || exit 1

{
    echo ""
    echo "=== [$(date -u +%FT%TZ)] Starting $TASK ==="
} >> "$LOG"

# One-shot container. Every Python call goes through it, notify_failure
# included, so the host needs only git and docker.
run() {
    "$DOCKER" compose -f "$COMPOSE" run --rm pipeline "$@"
}

notify() {
    # $1 = step name, $2 = exit code. Best-effort: a failed notification must
    # not mask the failure it is reporting.
    run python -m scripts.notify_failure "$TASK" "$1" "logs/$TASK.log" "$2" \
        >> "$LOG" 2>&1 || true
}

# --- Guard: an unset or empty DATABASE_URL means SQLite ------------------
# database.resolve_database_url() falls back to DB_PATH, which in a fresh
# container is an empty file. The job would read nothing, write nothing and
# exit zero -- the failure this repo has already been bitten by.
if ! run python -c "
import os, sys
url = os.environ.get('DATABASE_URL') or ''
if not url.startswith('postgres'):
    sys.exit('DATABASE_URL is unset, empty, or not a Postgres URL')
" >> "$LOG" 2>&1; then
    notify database_url_guard 1
    exit 1
fi

# --- Guard: R2 retention, warned about but never fatal -------------------
# If any of the four R2_* values is missing, retention is a silent no-op: the
# run fetches, extracts, goes green and keeps nothing. These are the plans no
# other machine can re-fetch, so their PDFs are the least recoverable in the
# corpus. Warn loudly and continue -- refusing to run would mean fetching
# nothing at all, which is strictly worse than fetching without retention.
if ! run python -c "
import os, sys
missing = [n for n in ('R2_ACCOUNT_ID', 'R2_ACCESS_KEY_ID',
                       'R2_SECRET_ACCESS_KEY', 'R2_BUCKET')
           if not os.environ.get(n)]
if missing:
    sys.exit('PDF retention OFF -- missing: %s' % ', '.join(missing))
" >> "$LOG" 2>&1; then
    echo "[$(date -u +%FT%TZ)] WARNING: retention is off, PDFs will not be kept" >> "$LOG"
    notify r2_retention_off 1
fi

# --- Keep the block lists and plan registry current ----------------------
echo "[$(date -u +%FT%TZ)] git pull --rebase" >> "$LOG"
if ! "$GIT" pull --rebase origin master >> "$LOG" 2>&1; then
    echo "[$(date -u +%FT%TZ)] pull failed, aborting rebase" >> "$LOG"
    "$GIT" rebase --abort >> "$LOG" 2>&1
    notify git_pull 1
    exit 1
fi

# --- Board materials: fetch + extract + summarize ------------------------
# `docker compose` writes its own progress to stderr, so stdout is just the
# one line the helper prints. 2>/dev/null keeps a compose warning out of the
# captured value.
MATERIALS_IDS=$(run python -m scripts.waf_blocked_ids --materials 2>/dev/null | tr -d '\r')
if [ -z "$MATERIALS_IDS" ]; then
    notify resolve_materials_ids 1
    exit 1
fi

echo "[$(date -u +%FT%TZ)] pipeline.py $MATERIALS_IDS" >> "$LOG"
# Capture the status into a variable rather than testing with `if ! ...`.
# Inside an `if ! cmd; then` body, `$?` is the status of the *negation* --
# always 0 -- so the notification would report success for a failed step.
# shellcheck disable=SC2086
run python pipeline.py $MATERIALS_IDS >> "$LOG" 2>&1
rc=$?
if [ "$rc" -ne 0 ]; then
    notify pipeline "$rc"
    # Not fatal: the CAFR refresh is independent and worth attempting.
    echo "[$(date -u +%FT%TZ)] pipeline exited $rc, continuing to CAFRs" >> "$LOG"
fi

# --- CAFRs ---------------------------------------------------------------
CAFR_IDS=$(run python -m scripts.waf_blocked_ids --cafr 2>/dev/null | tr -d '\r')
if [ -z "$CAFR_IDS" ]; then
    notify resolve_cafr_ids 1
    exit 1
fi

echo "[$(date -u +%FT%TZ)] refresh_cafrs.py $CAFR_IDS" >> "$LOG"
# shellcheck disable=SC2086
run python refresh_cafrs.py $CAFR_IDS >> "$LOG" 2>&1
rc=$?
if [ "$rc" -ne 0 ]; then
    notify refresh_cafrs "$rc"
    exit 1
fi

echo "=== [$(date -u +%FT%TZ)] $TASK completed ===" >> "$LOG"
exit 0
