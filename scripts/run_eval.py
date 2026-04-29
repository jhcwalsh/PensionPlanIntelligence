"""
Run the RFP pipeline against fixture documents, compare predictions to the
golden set, and fail if accuracy regresses more than 2pp from the baseline.

Used by .github/workflows/nightly_eval.yml. Exit codes:
    0 — within tolerance (or no baseline yet, results written)
    1 — regression > 2pp; CI fails
    2 — internal error

Usage:
    python -m scripts.run_eval [--update-baseline]

When --update-baseline is set, the script overwrites
fixtures/eval_baseline.json with the new accuracy on success. Used by the
nightly job after a successful run.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
FIXTURE_DOCS = REPO / "fixtures" / "documents"
FIXTURE_LLM = REPO / "fixtures" / "llm_responses"
GOLDEN = REPO / "fixtures" / "golden_set.jsonl"
BASELINE = REPO / "fixtures" / "eval_baseline.json"

REGRESSION_TOLERANCE_PP = 2.0   # percentage points


def _seed_db_with_fixtures():
    """Spin up an in-memory pipeline run against fixture text files."""
    import database as db

    # Use a temp DB so we never touch real data.
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    os.environ["DB_PATH"] = tmp.name

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine(f"sqlite:///{tmp.name}")
    db.engine = engine
    db.SessionLocal = sessionmaker(bind=engine)
    db.Base.metadata.create_all(engine)

    session = db.get_session()
    try:
        for plan_id, name in [("calpers", "CalPERS"), ("calstrs", "CalSTRS")]:
            session.add(db.Plan(id=plan_id, name=name, abbreviation=name, state="CA"))

        for doc_id, plan_id, fname, url in [
            (1, "calpers", "calpers_2024_board.txt",
             "https://www.calpers.ca.gov/board/2024-03/packet.pdf"),
            (2, "calstrs", "calstrs_2024_investment.txt",
             "https://www.calstrs.com/board/2024-04/packet.pdf"),
            (3, "calpers", "calpers_2024_governance.txt",
             "https://www.calpers.ca.gov/governance/2024-02/packet.pdf"),
        ]:
            text = (FIXTURE_DOCS / fname).read_text()
            session.add(db.Document(
                id=doc_id, plan_id=plan_id, url=url, filename=fname,
                doc_type="board_pack",
                # Non-existent path forces the diagnostic to use the cached
                # extracted_text rather than trying to re-open a real PDF.
                local_path="/nonexistent/path/" + fname,
                extracted_text=text, extraction_status="done",
                page_count=text.count("[Page "),
            ))
        session.commit()
    finally:
        session.close()

    return tmp.name


def _dump_predictions(out_path: Path) -> int:
    import database as db
    session = db.get_session()
    try:
        rows = session.query(db.RFPRecord).all()
        with out_path.open("w") as f:
            for r in rows:
                f.write(r.record + "\n")
        return len(rows)
    finally:
        session.close()


def _load_baseline_accuracy() -> float | None:
    if not BASELINE.exists():
        return None
    try:
        return float(json.loads(BASELINE.read_text())["overall_accuracy"])
    except (KeyError, ValueError, json.JSONDecodeError):
        return None


def _write_baseline(accuracy: float, field_accuracy: dict[str, float]) -> None:
    BASELINE.write_text(json.dumps({
        "overall_accuracy": accuracy,
        "field_accuracy": field_accuracy,
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }, indent=2) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--update-baseline", action="store_true",
                        help="Overwrite fixtures/eval_baseline.json on success.")
    args = parser.parse_args()

    os.environ.setdefault("LLM_MODE", "mock")
    os.environ.setdefault("LLM_FIXTURE_DIR", str(FIXTURE_LLM))

    _seed_db_with_fixtures()

    from rfp.orchestrator import run_rfp_extraction
    run_rfp_extraction()

    with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False, mode="w") as f:
        pred_path = Path(f.name)
    n = _dump_predictions(pred_path)
    print(f"Wrote {n} predictions to {pred_path}")

    from lib.eval_harness import evaluate
    result = evaluate(GOLDEN, pred_path)
    print(json.dumps({
        "overall_accuracy": result.overall_accuracy,
        "field_accuracy": result.field_accuracy,
        "matched_pairs": result.matched_pairs,
        "false_positives": result.false_positives,
        "false_negatives": result.false_negatives,
    }, indent=2))

    baseline = _load_baseline_accuracy()
    if baseline is None:
        print("No baseline found; writing one.")
        _write_baseline(result.overall_accuracy, result.field_accuracy)
        return 0

    drop_pp = (baseline - result.overall_accuracy) * 100.0
    print(f"Baseline {baseline:.4f}, current {result.overall_accuracy:.4f}, "
          f"drop {drop_pp:+.2f}pp (tolerance {REGRESSION_TOLERANCE_PP}pp)")

    if drop_pp > REGRESSION_TOLERANCE_PP:
        print(f"REGRESSION: accuracy dropped by {drop_pp:.2f}pp", file=sys.stderr)
        return 1

    if args.update_baseline:
        _write_baseline(result.overall_accuracy, result.field_accuracy)
        print(f"Updated baseline at {BASELINE}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
