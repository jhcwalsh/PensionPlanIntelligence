"""Generate per-plan CAFR summary markdown files.

For every plan with a `cafr_extract` row, writes a Markdown summary to
``cafr_summaries/<plan_id>.md`` containing:

  - Header with plan name, fiscal year, source URL
  - Asset Allocation table (asset class · target · actual · range · drift)
  - Performance table (scope · period · return · benchmark · vs benchmark)
  - Investment policy text (verbatim from extract)

Also writes ``cafr_summaries/index.md`` linking to every per-plan summary
plus a status row for plans without an extract.

Usage:
    python export_cafr_summaries.py             # all extracted plans
    python export_cafr_summaries.py opers ipers # specific plans only
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from rich.console import Console

from database import (
    CafrAllocation,
    CafrExtract,
    CafrPerformance,
    Document,
    Plan,
    get_session,
    init_db,
)

console = Console(legacy_windows=False)
OUTPUT_DIR = Path(__file__).parent / "cafr_summaries"


def _fmt_pct(v: float | None, sign: bool = False) -> str:
    if v is None:
        return ""
    return f"{v:+.2f}%" if sign else f"{v:.2f}%"


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    """Render a GitHub-flavoured Markdown table."""
    if not rows:
        return ""
    head = "| " + " | ".join(headers) + " |"
    sep = "| " + " | ".join("---" for _ in headers) + " |"
    body = "\n".join("| " + " | ".join(r) + " |" for r in rows)
    return "\n".join([head, sep, body])


def _alloc_rows(allocs: list[CafrAllocation]) -> list[list[str]]:
    rows = []
    for a in allocs:
        drift = (
            f"{a.actual_pct - a.target_pct:+.2f}%"
            if a.actual_pct is not None and a.target_pct is not None
            else ""
        )
        range_str = ""
        if a.target_range_low is not None or a.target_range_high is not None:
            lo = _fmt_pct(a.target_range_low) or "—"
            hi = _fmt_pct(a.target_range_high) or "—"
            range_str = f"{lo} – {hi}"
        rows.append([
            a.asset_class,
            _fmt_pct(a.target_pct),
            _fmt_pct(a.actual_pct),
            range_str,
            drift,
            (a.notes or "").replace("|", "\\|"),
        ])
    return rows


def _perf_rows(perfs: list[CafrPerformance]) -> list[list[str]]:
    rows = []
    for p in perfs:
        vs = (
            f"{p.return_pct - p.benchmark_return_pct:+.2f}%"
            if p.return_pct is not None and p.benchmark_return_pct is not None
            else ""
        )
        rows.append([
            p.scope,
            p.period,
            _fmt_pct(p.return_pct),
            _fmt_pct(p.benchmark_return_pct),
            (p.benchmark_name or "").replace("|", "\\|"),
            vs,
            (p.notes or "").replace("|", "\\|"),
        ])
    return rows


def _summary_for_plan(session, plan: Plan) -> tuple[str, dict] | None:
    """Build the markdown summary for one plan. Returns (markdown, meta) or None
    if the plan has no extract yet."""
    extract = (
        session.query(CafrExtract)
        .filter(CafrExtract.plan_id == plan.id)
        .order_by(CafrExtract.fiscal_year.desc(), CafrExtract.id.desc())
        .first()
    )
    if extract is None:
        return None

    allocs = (
        session.query(CafrAllocation)
        .filter(CafrAllocation.cafr_extract_id == extract.id)
        .order_by(CafrAllocation.id)
        .all()
    )
    perfs = (
        session.query(CafrPerformance)
        .filter(CafrPerformance.cafr_extract_id == extract.id)
        .order_by(CafrPerformance.scope, CafrPerformance.period)
        .all()
    )
    document = (
        session.query(Document).filter_by(id=extract.document_id).first()
    )

    plan_label = plan.abbreviation or plan.name
    fy = extract.fiscal_year or "—"

    parts: list[str] = []
    parts.append(f"# {plan_label} — CAFR FY{fy}")
    parts.append(f"_{plan.name}_  ")
    if plan.state:
        parts.append(f"**State:** {plan.state}  ")
    if plan.aum_billions:
        parts.append(f"**AUM:** ${plan.aum_billions:.1f}B  ")

    if document and document.url:
        parts.append(f"**Source:** [{document.filename or document.url}]"
                     f"({document.url})")
    if extract.pages_used:
        parts.append(f"**Pages used:** {extract.pages_used}  ")
    if extract.model_used:
        parts.append(f"**Model:** {extract.model_used}  ")
    if extract.extracted_at:
        parts.append(
            f"**Extracted:** {extract.extracted_at.strftime('%Y-%m-%d')}  "
        )
    parts.append("")

    # Asset Allocation
    parts.append("## Asset Allocation")
    if allocs:
        parts.append(_md_table(
            ["Asset class", "Target", "Actual", "Range", "Drift", "Notes"],
            _alloc_rows(allocs),
        ))
        targets = [a.target_pct for a in allocs if a.target_pct is not None]
        if targets:
            total = sum(targets)
            sentinel = "" if abs(total - 100.0) <= 1.0 else " ⚠ off from 100%"
            parts.append("")
            parts.append(f"_Targets sum to {total:.2f}%.{sentinel}_")
    else:
        parts.append("_No asset-allocation rows extracted._")
    parts.append("")

    # Performance
    parts.append("## Performance")
    if perfs:
        parts.append(_md_table(
            ["Scope", "Period", "Return", "Benchmark return",
             "Benchmark name", "vs Benchmark", "Notes"],
            _perf_rows(perfs),
        ))
    else:
        parts.append("_No performance rows extracted._")
    parts.append("")

    if extract.investment_policy_text:
        parts.append("## Investment Policy Text")
        parts.append("```")
        parts.append(extract.investment_policy_text.strip())
        parts.append("```")
        parts.append("")

    parts.append("---")
    parts.append(f"_Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}._")

    md = "\n".join(parts)
    meta = {
        "plan_label": plan_label,
        "plan_name": plan.name,
        "fy": fy,
        "n_allocations": len(allocs),
        "n_performance": len(perfs),
        "extracted_at": extract.extracted_at,
    }
    return md, meta


def _build_index(per_plan_meta: dict[str, dict]) -> str:
    """Build cafr_summaries/index.md linking to every per-plan summary."""
    lines: list[str] = []
    lines.append("# CAFR Summaries")
    lines.append("")
    lines.append(
        f"_{len(per_plan_meta)} plan summaries · generated "
        f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}_"
    )
    lines.append("")
    lines.append(_md_table(
        ["Plan", "Name", "FY", "# asset classes", "# perf rows", "Extracted"],
        [
            [
                f"[{m['plan_label']}]({pid}.md)",
                m["plan_name"],
                str(m["fy"]),
                str(m["n_allocations"]),
                str(m["n_performance"]),
                m["extracted_at"].strftime("%Y-%m-%d") if m["extracted_at"] else "—",
            ]
            for pid, m in sorted(per_plan_meta.items(),
                                 key=lambda kv: kv[1]["plan_label"])
        ],
    ))
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export per-plan CAFR summary markdown files."
    )
    parser.add_argument(
        "plan_ids", nargs="*",
        help="Plan IDs to export (default: all with cafr_extract rows).",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=OUTPUT_DIR,
        help=f"Where to write summaries (default: {OUTPUT_DIR}).",
    )
    args = parser.parse_args()

    init_db()
    session = get_session()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.plan_ids:
        plans = (
            session.query(Plan).filter(Plan.id.in_(args.plan_ids))
            .order_by(Plan.name).all()
        )
    else:
        # Only plans with at least one cafr_extract row
        extracted_pids = {
            r[0] for r in
            session.query(CafrExtract.plan_id).distinct().all()
        }
        plans = (
            session.query(Plan).filter(Plan.id.in_(extracted_pids))
            .order_by(Plan.name).all()
        )

    if not plans:
        console.print("[yellow]No plans matched.[/yellow]")
        return 1

    written = 0
    skipped = 0
    per_plan_meta: dict[str, dict] = {}

    for plan in plans:
        result = _summary_for_plan(session, plan)
        if result is None:
            console.print(
                f"  [dim]{plan.id}: no CAFR extract — skipped[/dim]"
            )
            skipped += 1
            continue
        md, meta = result
        out_path = args.output_dir / f"{plan.id}.md"
        out_path.write_text(md, encoding="utf-8")
        written += 1
        per_plan_meta[plan.id] = meta
        console.print(
            f"  [green]{plan.id}[/green] FY{meta['fy']} "
            f"({meta['n_allocations']} alloc / {meta['n_performance']} perf)"
            f" -> {out_path}"
        )

    if per_plan_meta:
        index_path = args.output_dir / "index.md"
        index_path.write_text(_build_index(per_plan_meta), encoding="utf-8")
        console.print(f"\n  [bold]index[/bold] -> {index_path}")

    console.rule(f"[bold green]{written} written, {skipped} skipped[/bold green]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
