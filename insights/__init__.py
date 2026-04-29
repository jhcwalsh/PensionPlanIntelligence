"""CIO Insights publishing automation.

Orchestrates the weekly / monthly / annual publication cadence with a
founder-only magic-link approval flow.

Entry point: ``python -m insights.scheduler {weekly|monthly|annual|reminders}``

Modules:
    config     environment + mock-mode plumbing
    compose    adapters that drive existing summarizer/generate_notes
    render     PDF rendering (lifted from app.py — same logic)
    approval   token lifecycle + approval email content
    publish    git commit-and-push to deploy branch
    notify     Slack failure alerts
    weekly     weekly cycle (scrape → extract → compose → email)
    monthly    monthly cycle (composes from 4 approved weeklies)
    annual    annual cycle (composes from 12 approved monthlies)
    scheduler  CLI entry point
"""
