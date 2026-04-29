"""
Golden-set evaluation harness for structured RFP extraction.

Compares predicted records against hand-verified golden records,
matching by best-key (plan_id, rfp_type, nearest date) and computing
field-by-field accuracy with per-type tolerances:

- Strings: case-insensitive equality, with Levenshtein <=2 considered a
  match for resilience to minor spelling drift.
- Numbers: within +/-5%.
- Dates: within +/-7 days.
- Lists: order-insensitive set equality after string normalization.

Usage:
    from lib.eval_harness import evaluate
    result = evaluate("fixtures/golden_set.jsonl", "/tmp/preds.jsonl", "rfp")
    print(result.overall_accuracy)
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

NUMERIC_TOLERANCE = 0.05      # ±5%
DATE_TOLERANCE_DAYS = 7
STRING_LEV_TOLERANCE = 2

# Fields we score. Operational metadata (rfp_id, source_document, etc.) is
# excluded from per-field accuracy because it's mechanical.
SCORED_FIELDS = (
    "rfp_type", "title", "status",
    "release_date", "response_due_date", "award_date",
    "mandate_size_usd_millions", "asset_class",
    "incumbent_manager", "shortlisted_managers", "awarded_manager",
)


@dataclass
class FieldComparison:
    field: str
    expected: Any
    actual: Any
    matched: bool


@dataclass
class RecordComparison:
    matched_to: str | None
    fields: list[FieldComparison] = field(default_factory=list)


@dataclass
class EvalResult:
    overall_accuracy: float
    field_accuracy: dict[str, float]
    false_positives: int       # predictions with no matching golden
    false_negatives: int       # goldens with no matching prediction
    matched_pairs: int
    per_record: list[RecordComparison] = field(default_factory=list)


def _load_jsonl(path: str | Path) -> list[dict]:
    p = Path(path)
    rows = []
    with p.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        for j, cb in enumerate(b, 1):
            cost = 0 if ca == cb else 1
            curr.append(min(curr[-1] + 1, prev[j] + 1, prev[j - 1] + cost))
        prev = curr
    return prev[-1]


def _norm_str(s: Any) -> str:
    return ("" if s is None else str(s)).strip().lower()


def _strings_match(a: Any, b: Any) -> bool:
    na, nb = _norm_str(a), _norm_str(b)
    if na == nb:
        return True
    if not na or not nb:
        return False
    return _levenshtein(na, nb) <= STRING_LEV_TOLERANCE


def _numbers_match(a: Any, b: Any) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    a_f, b_f = float(a), float(b)
    if a_f == b_f:
        return True
    denom = max(abs(a_f), abs(b_f), 1e-9)
    return abs(a_f - b_f) / denom <= NUMERIC_TOLERANCE


def _parse_date(d: Any) -> date | None:
    if d is None or d == "":
        return None
    if isinstance(d, date):
        return d
    try:
        return datetime.fromisoformat(str(d)).date()
    except ValueError:
        return None


def _dates_match(a: Any, b: Any) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    da, db = _parse_date(a), _parse_date(b)
    if da is None or db is None:
        return False
    return abs((da - db).days) <= DATE_TOLERANCE_DAYS


def _lists_match(a: Any, b: Any) -> bool:
    a = a or []
    b = b or []
    if len(a) != len(b):
        return False
    a_norm = sorted(_norm_str(x) for x in a)
    b_norm = sorted(_norm_str(x) for x in b)
    return all(
        ax == bx or _levenshtein(ax, bx) <= STRING_LEV_TOLERANCE
        for ax, bx in zip(a_norm, b_norm)
    )


def _fields_match(field_name: str, expected: Any, actual: Any) -> bool:
    if field_name in {"release_date", "response_due_date", "award_date"}:
        return _dates_match(expected, actual)
    if field_name == "mandate_size_usd_millions":
        return _numbers_match(expected, actual)
    if field_name == "shortlisted_managers":
        return _lists_match(expected, actual)
    return _strings_match(expected, actual)


def _match_score(golden: dict, pred: dict) -> int:
    """Higher is more similar. Used to align goldens to predictions."""
    score = 0
    if golden.get("plan_id") == pred.get("plan_id"):
        score += 100
    if golden.get("rfp_type") == pred.get("rfp_type"):
        score += 50
    # Best date proximity contributes up to 30
    for k in ("release_date", "response_due_date", "award_date"):
        if _dates_match(golden.get(k), pred.get(k)) and golden.get(k):
            score += 10
    if _strings_match(golden.get("title"), pred.get("title")):
        score += 20
    return score


def _greedy_align(golden: list[dict], pred: list[dict]) -> list[tuple[int, int]]:
    """Return list of (golden_idx, pred_idx) pairings; unmatched indices excluded."""
    pairs: list[tuple[int, int]] = []
    used_pred: set[int] = set()
    used_gold: set[int] = set()
    candidates = []
    for gi, g in enumerate(golden):
        for pi, p in enumerate(pred):
            score = _match_score(g, p)
            if score > 0:
                candidates.append((score, gi, pi))
    candidates.sort(reverse=True)
    for score, gi, pi in candidates:
        # Require a minimum score to avoid pairing unrelated records.
        if score < 100:
            continue
        if gi in used_gold or pi in used_pred:
            continue
        pairs.append((gi, pi))
        used_gold.add(gi)
        used_pred.add(pi)
    return pairs


def evaluate(
    golden_path: str | Path,
    pred_path: str | Path,
    profile: str = "rfp",
) -> EvalResult:
    """
    Compare predicted records against golden records and return an EvalResult.

    `profile` is currently only used for forward-compatibility with
    additional task profiles; the scoring rules above are RFP-tuned.
    """
    golden = _load_jsonl(golden_path)
    pred = _load_jsonl(pred_path)

    pairs = _greedy_align(golden, pred)
    matched_count = len(pairs)
    fp = len(pred) - matched_count
    fn = len(golden) - matched_count

    field_correct: dict[str, int] = {f: 0 for f in SCORED_FIELDS}
    field_total: dict[str, int] = {f: 0 for f in SCORED_FIELDS}
    per_record: list[RecordComparison] = []

    for gi, pi in pairs:
        rc = RecordComparison(matched_to=pred[pi].get("rfp_id"))
        for f_name in SCORED_FIELDS:
            ev = golden[gi].get(f_name)
            av = pred[pi].get(f_name)
            ok = _fields_match(f_name, ev, av)
            field_total[f_name] += 1
            if ok:
                field_correct[f_name] += 1
            rc.fields.append(FieldComparison(field=f_name, expected=ev, actual=av, matched=ok))
        per_record.append(rc)

    field_accuracy = {
        f: (field_correct[f] / field_total[f]) if field_total[f] > 0 else 0.0
        for f in SCORED_FIELDS
    }
    correct_total = sum(field_correct.values())
    field_total_sum = sum(field_total.values())
    matched_quality = (correct_total / field_total_sum) if field_total_sum else 0.0

    # Overall accuracy penalises FPs and FNs by treating them as 0 score
    # over the same denominator.
    denom = max(1, len(golden) + fp)
    overall = (matched_count * matched_quality) / denom

    return EvalResult(
        overall_accuracy=round(overall, 4),
        field_accuracy={k: round(v, 4) for k, v in field_accuracy.items()},
        false_positives=fp,
        false_negatives=fn,
        matched_pairs=matched_count,
        per_record=per_record,
    )
