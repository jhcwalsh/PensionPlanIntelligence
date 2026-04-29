"""rfp_id is a stable hash of (plan, type, anchor_date, normalized_title)."""

from __future__ import annotations

from rfp.ids import compute_rfp_id, normalize_title


def test_same_inputs_produce_same_id():
    a = compute_rfp_id("calpers", "Consultant", "2024-03-15", "General Consultant")
    b = compute_rfp_id("calpers", "Consultant", "2024-03-15", "General Consultant")
    assert a == b
    assert len(a) == 16


def test_normalization_collapses_whitespace_and_case():
    a = compute_rfp_id("calpers", "Consultant", "2024-03-15", "General Consultant")
    b = compute_rfp_id("calpers", "Consultant", "2024-03-15", "  GENERAL   consultant  ")
    assert a == b


def test_normalization_strips_punctuation():
    a = compute_rfp_id("calpers", "Consultant", "2024-03-15", "General Consultant")
    b = compute_rfp_id("calpers", "Consultant", "2024-03-15", "General Consultant.")
    assert a == b


def test_different_plan_produces_different_id():
    a = compute_rfp_id("calpers", "Consultant", "2024-03-15", "General Consultant")
    b = compute_rfp_id("calstrs", "Consultant", "2024-03-15", "General Consultant")
    assert a != b


def test_different_type_produces_different_id():
    a = compute_rfp_id("calpers", "Consultant", "2024-03-15", "General Consultant")
    b = compute_rfp_id("calpers", "Manager", "2024-03-15", "General Consultant")
    assert a != b


def test_null_anchor_date_is_stable():
    a = compute_rfp_id("calpers", "Consultant", None, "General Consultant")
    b = compute_rfp_id("calpers", "Consultant", None, "General Consultant")
    assert a == b


def test_normalize_title_strips_punctuation():
    assert normalize_title("General-Investment Consultant!") == "general investment consultant"
