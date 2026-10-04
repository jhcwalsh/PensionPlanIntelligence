#!/usr/bin/env bash
# PreToolUse hook (Bash): refuse a `git commit` in this repo unless
# SESSION_NOTES.md goes into that commit. CLAUDE.md "Session notes" is the
# rule; this is what makes it stick. Exit 2 blocks the command and hands the
# message on stderr back to Claude, which then updates the notes and retries.
#
# Lets through: commands that do not RUN a commit, merge commits (a base merge
# carries no new work to note), and a commit whose own command stages
# SESSION_NOTES.md (`git add SESSION_NOTES.md && ...`, `git add -A`,
# `git add .`, or `commit -a` on a tracked, modified file).
#
# "Runs a commit" means `git ... commit` at a place the shell would execute it:
# the start of a line, or after && || ; | ( $( { then do. Text that only
# mentions a commit - a heredoc body, a quoted string, a comment, a file being
# written - does not count. Without that, writing this very script blocked.
#
# Needs only bash, grep, sed, awk and git (jq is used when present), so it runs
# the same in a cloud container and in Git Bash on Windows.
# Shared across James's repos: change it everywhere, not in one repo.

input=$(cat)

# ---- the command, out of the hook's JSON
if command -v jq >/dev/null 2>&1; then
  cmd=$(printf '%s' "$input" | jq -r '.tool_input.command // empty' 2>/dev/null)
else
  cmd=$(printf '%s' "$input" \
    | grep -oE '"command"[[:space:]]*:[[:space:]]*"(\\.|[^"\\])*"' | head -n 1 \
    | sed -E 's/^"command"[[:space:]]*:[[:space:]]*"//; s/"$//' \
    | sed -e 's/\\n/\n/g' -e 's/\\t/\t/g' -e 's/\\"/"/g' -e 's/\\\\/\\/g')
fi
[ -n "$cmd" ] || exit 0

# ---- the lines the shell would actually run: heredoc bodies dropped,
# quoted strings and comments blanked
runnable=$(printf '%s\n' "$cmd" | awk '
  term != "" { t = $0; sub(/^[ \t]+/, "", t); sub(/[ \t]+$/, "", t); if (t == term) term = ""; next }
  {
    line = $0
    if (match(line, /<<-?[ \t]*["\047]?[A-Za-z_][A-Za-z0-9_]*["\047]?/)) {
      tag = substr(line, RSTART, RLENGTH); gsub(/^<<-?[ \t]*|["\047]/, "", tag); term = tag
    }
    gsub(/"([^"\\]|\\.)*"/, "\"\"", line)
    gsub(/\047[^\047]*\047/, "\047\047", line)
    sub(/(^|[ \t])#.*$/, "", line)
    print line
  }')

pos='(^|&&|\|\||;|\||\(|\$\(|\{|[[:space:]]then|[[:space:]]do)[[:space:]]*'
gitcmd='git([[:space:]]+-[^[:space:]]+([[:space:]]+[^-[:space:]][^[:space:]]*)?)*[[:space:]]+'
printf '%s\n' "$runnable" | grep -Eq "${pos}${gitcmd}commit([[:space:]]|$)" || exit 0

# ---- which repo the commit lands in: `git -C dir`, else the last `cd dir`
# before it, else the directory the command runs in. Relative paths resolve
# from there. Only a repo that carries this hook has opted in to the rule.
if command -v jq >/dev/null 2>&1; then
  here=$(printf '%s' "$input" | jq -r '.cwd // empty' 2>/dev/null)
else
  here=$(printf '%s' "$input" | grep -oE '"cwd"[[:space:]]*:[[:space:]]*"(\\.|[^"\\])*"' | head -n 1 \
    | sed -E 's/^"cwd"[[:space:]]*:[[:space:]]*"//; s/"$//; s/\\\\/\\/g')
fi
here="${here:-${CLAUDE_PROJECT_DIR:-$(pwd)}}"
before=$(printf '%s\n' "$runnable" | awk -v re="${gitcmd}commit" '{ print } $0 ~ re { exit }')
target=$(printf '%s\n' "$before" | grep -oE "git[[:space:]]+-C[[:space:]]+[^[:space:]&;|]+" | tail -n 1 | sed -E 's/^git[[:space:]]+-C[[:space:]]+//')
[ -n "$target" ] || target=$(printf '%s\n' "$before" | grep -oE "(^|&&|;|\(|\{)[[:space:]]*cd[[:space:]]+[^[:space:]&;|)]+" | tail -n 1 | sed -E 's/^.*cd[[:space:]]+//')
target=$(printf '%s' "$target" | sed -E "s/^[\"']//; s/[\"']$//")
case "$target" in
  "") repo="$here" ;;
  /*|[A-Za-z]:*) repo="$target" ;;
  "~"*) repo="$HOME${target#\~}" ;;
  *) repo="$here/$target" ;;
esac
repo=$(git -C "$repo" rev-parse --show-toplevel 2>/dev/null) || exit 0
[ -f "$repo/.claude/hooks/require-session-notes.sh" ] || exit 0
gitdir=$(git -C "$repo" rev-parse --absolute-git-dir)
[ -f "$gitdir/MERGE_HEAD" ] && exit 0

notes=SESSION_NOTES.md
git -C "$repo" diff --cached --name-only | grep -qx "$notes" && exit 0

# ---- staged by this same command?  Naming the notes file in a `git add` is
# trusted outright: the hook runs BEFORE the command, and the same command may
# write the entry first and stage it after. `-A`, `.` and `commit -a` show no
# such intent, so for those the notes must already be changed.
printf '%s\n' "$runnable" | grep -Eq "${pos}${gitcmd}add([[:space:]][^&;|]*)?[[:space:]]$notes([[:space:]]|$)" && exit 0
changed=$(git -C "$repo" status --porcelain -- "$notes")
if [ -n "$changed" ]; then
  printf '%s\n' "$runnable" | grep -Eq "${pos}${gitcmd}add([[:space:]][^&;|]*)?[[:space:]](-A|--all|\.)([[:space:]]|$)" && exit 0
  case "$changed" in
    '??'*) ;;                                   # untracked: -a does not add it
    *) printf '%s\n' "$runnable" | grep -Eq "${pos}${gitcmd}commit[^&;|]*[[:space:]]-[[:alpha:]]*a" && exit 0 ;;
  esac
fi

cat >&2 <<MSG
Blocked: SESSION_NOTES.md is not part of this commit.
CLAUDE.md "Session notes" says to update it before every commit and include
it in the same commit. Add a dated entry at the top (Done, Why, Broken/unsure,
Next) or a one-line append to today's entry for a small fix, stage it, then
commit again.
MSG
exit 2
