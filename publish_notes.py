"""
Regenerate notes and publish to the deployed Render site.

Typical local workflow:
    1. python pipeline.py        # fetch / extract / summarize new docs
    2. python publish_notes.py   # regenerate notes + commit + push

This script:
    - Regenerates all three notes (7-day highlights, 2026 agenda trends,
      CIO Insights) using existing DB data (no pipeline run)
    - Fact-checks the CIO Insights against the source corpus
    - Shows a diff summary of the changed note files
    - Commits to the current branch and pushes to origin

If the current branch is the repo's deploy branch (master), Render
auto-deploys after the push.

Flags:
    --yes / -y           Skip the confirmation prompt
    --strict-validate    Fail if the CIO Insights fact-check flags tokens
    --skip-generate      Don't regenerate; just commit whatever is in notes/
    --message / -m MSG   Custom commit message
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.prompt import Confirm

console = Console(legacy_windows=False)
ROOT = Path(__file__).parent


def run(cmd: list[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    """Run a shell command in the repo root. Streams output unless capture=True."""
    console.print(f"[dim]$ {' '.join(cmd)}[/dim]")
    kwargs: dict = {"cwd": str(ROOT)}
    if capture:
        kwargs["capture_output"] = True
        kwargs["text"] = True
    result = subprocess.run(cmd, **kwargs)
    if check and result.returncode != 0:
        if capture and result.stderr:
            console.print(f"[red]{result.stderr}[/red]")
        console.print(f"[red]Command failed ({result.returncode}): {' '.join(cmd)}[/red]")
        sys.exit(result.returncode)
    return result


def current_branch() -> str:
    return run(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture=True).stdout.strip()


def notes_status() -> str:
    """Porcelain status restricted to notes/ — empty string means clean."""
    return run(["git", "status", "--porcelain", "--", "notes/"], capture=True).stdout


def main():
    parser = argparse.ArgumentParser(description="Regenerate notes and publish to Render")
    parser.add_argument("--yes", "-y", action="store_true",
                        help="Skip confirmation prompt")
    parser.add_argument("--skip-generate", action="store_true",
                        help="Don't run generate_notes.py; just commit what's in notes/")
    parser.add_argument("--strict-validate", action="store_true",
                        help="Fail the publish if the CIO Insights fact-check "
                             "finds any unmatched tokens")
    parser.add_argument("--message", "-m",
                        help="Custom commit message")
    args = parser.parse_args()

    branch = current_branch()
    console.rule(f"[bold blue]Publish notes — branch: {branch}[/bold blue]")

    # Warn if there are unrelated unstaged/staged changes so we don't sweep them in
    dirty = run(["git", "status", "--porcelain"], capture=True).stdout
    non_notes_dirty = [l for l in dirty.splitlines() if l.strip() and "notes/" not in l]
    if non_notes_dirty:
        console.print("[yellow]Warning: uncommitted changes outside notes/ detected:[/yellow]")
        for line in non_notes_dirty[:10]:
            console.print(f"  {line}")
        console.print("[yellow]These will NOT be included in the publish commit.[/yellow]")

    # Regenerate
    if not args.skip_generate:
        cmd = [sys.executable, "generate_notes.py", "--skip-pipeline"]
        if args.strict_validate:
            cmd.append("--strict-validate")
        run(cmd)

    # Diff summary
    diff = run(["git", "diff", "--stat", "--", "notes/"], capture=True).stdout
    staged_diff = run(["git", "diff", "--cached", "--stat", "--", "notes/"], capture=True).stdout
    if diff or staged_diff:
        console.rule("[bold]Changes to publish[/bold]")
        if diff:
            console.print(diff.rstrip())
        if staged_diff:
            console.print("[dim]already staged:[/dim]")
            console.print(staged_diff.rstrip())
    else:
        console.print("[yellow]No changes to notes/ — nothing to publish.[/yellow]")
        return

    if not args.yes:
        if not Confirm.ask(f"Commit and push to origin/{branch}?", default=True):
            console.print("[yellow]Aborted. Changes left in working tree.[/yellow]")
            return

    # Commit & push (only stage notes/, nothing else)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    msg = args.message or f"Refresh published notes ({today})"

    run(["git", "add", "--", "notes/"])
    # Check that staging produced something to commit (notes/ may have been
    # already staged above, or a file may have been auto-stripped).
    if not run(["git", "diff", "--cached", "--stat", "--", "notes/"],
               capture=True).stdout.strip():
        console.print("[yellow]Nothing staged after git add notes/. Exiting.[/yellow]")
        return

    run(["git", "commit", "-m", msg])
    run(["git", "push", "-u", "origin", branch])

    console.rule("[bold green]Published[/bold green]")
    if branch == "master":
        console.print("Render auto-deploy should kick off shortly at:")
        console.print("  https://pensionplanintelligence.onrender.com")
    else:
        console.print(f"Pushed to branch '{branch}'. Merge to master to deploy.")


if __name__ == "__main__":
    main()
