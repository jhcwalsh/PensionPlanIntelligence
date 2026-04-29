"""Eval harness scoring logic — match alignment, FP/FN counting, field tolerances."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lib.eval_harness import (
    NUMERIC_TOLERANCE, evaluate, _dates_match, _lists_match, _numbers_match,
    _strings_match, _greedy_align,
)

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"


def _golden_record(plan="calpers", rfp_type="Consultant", title="Investment Consulting Services",
                   release_date="2024-03-15", award_date=None,
                   mandate=1.2, incumbent="Wilshire Associates",
                   shortlisted=None, awarded=None) -> dict:
    return {
        "rfp_id": "x",
        "plan_id": plan,
        "rfp_type": rfp_type,
        "title": title,
        "status": "Planned" if not awarded else "Awarded",
        "release_date": release_date,
        "response_due_date": None,
        "award_date": award_date,
        "mandate_size_usd_millions": mandate,
        "asset_class": None,
        "incumbent_manager": incumbent,
        "incumbent_manager_id": None,
        "shortlisted_managers": shortlisted or [],
        "awarded_manager": awarded,
    }


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n")


def test_strings_match_lev_tolerance():
    assert _strings_match("Wilshire Associates", "Wilshire  Associates")
    assert _strings_match("BlackRock", "blackrock")
    assert _strings_match("BlackRock", "BlckRok")   # 2 edits
    assert not _strings_match("BlackRock", "Vanguard")


def test_numbers_match_within_5pct():
    assert _numbers_match(100, 104)
    assert _numbers_match(100, 95.1)
    assert not _numbers_match(100, 90)
    assert _numbers_match(None, None)
    assert not _numbers_match(None, 1)


def test_dates_match_within_7_days():
    assert _dates_match("2024-03-15", "2024-03-20")
    assert _dates_match("2024-03-15", "2024-03-08")
    assert not _dates_match("2024-03-15", "2024-04-01")
    assert _dates_match(None, None)
    assert not _dates_match(None, "2024-03-15")


def test_lists_match_set_equality():
    assert _lists_match(["A", "B", "C"], ["c", "a", "b"])
    assert _lists_match([], [])
    assert not _lists_match(["A"], ["A", "B"])


def test_evaluate_perfect_match(tmp_path):
    rows = [_golden_record()]
    _write_jsonl(tmp_path / "g.jsonl", rows)
    _write_jsonl(tmp_path / "p.jsonl", rows)
    result = evaluate(tmp_path / "g.jsonl", tmp_path / "p.jsonl")
    assert result.matched_pairs == 1
    assert result.false_positives == 0
    assert result.false_negatives == 0
    assert result.overall_accuracy == 1.0


def test_evaluate_one_field_drift(tmp_path):
    g = _golden_record(mandate=1.2)
    p = _golden_record(mandate=1.5)   # >5% off
    _write_jsonl(tmp_path / "g.jsonl", [g])
    _write_jsonl(tmp_path / "p.jsonl", [p])
    result = evaluate(tmp_path / "g.jsonl", tmp_path / "p.jsonl")
    assert result.matched_pairs == 1
    assert result.field_accuracy["mandate_size_usd_millions"] == 0.0
    # Other 10 fields match → overall < 1
    assert 0.8 < result.overall_accuracy < 1.0


def test_evaluate_false_positive(tmp_path):
    g = _golden_record()
    p1 = _golden_record()
    p2 = _golden_record(plan="florida_sba", rfp_type="Audit",
                         title="Audit RFP")
    _write_jsonl(tmp_path / "g.jsonl", [g])
    _write_jsonl(tmp_path / "p.jsonl", [p1, p2])
    result = evaluate(tmp_path / "g.jsonl", tmp_path / "p.jsonl")
    assert result.matched_pairs == 1
    assert result.false_positives == 1
    assert result.false_negatives == 0


def test_evaluate_false_negative(tmp_path):
    g1 = _golden_record()
    g2 = _golden_record(plan="calstrs", rfp_type="Actuary",
                         title="Actuarial RFP")
    _write_jsonl(tmp_path / "g.jsonl", [g1, g2])
    _write_jsonl(tmp_path / "p.jsonl", [_golden_record()])
    result = evaluate(tmp_path / "g.jsonl", tmp_path / "p.jsonl")
    assert result.matched_pairs == 1
    assert result.false_negatives == 1


def test_evaluate_real_golden_set_against_itself(tmp_path):
    """Self-eval of fixtures/golden_set.jsonl should be 100%."""
    golden = FIXTURES / "golden_set.jsonl"
    pred = tmp_path / "p.jsonl"
    pred.write_text(golden.read_text())
    result = evaluate(golden, pred)
    assert result.overall_accuracy == 1.0
    assert result.matched_pairs == 3
    assert result.false_positives == 0
    assert result.false_negatives == 0
