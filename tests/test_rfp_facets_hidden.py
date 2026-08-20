"""Frozen RFP-derived relationships are withheld from display.

rfp_records has been frozen since 2026-08-16: the extraction code is gone and
nothing refreshes the 189 rows, so anything derived from them never advances
its freshness date. A stale consultant or actuary reads as a current one.

The rows stay — scripts/build_manager_roster still consumes them — and so does
the snapshot. Only the display is filtered.

See docs/superpowers/specs/2026-08-19-portal-readiness-design.md §5.
"""

from __future__ import annotations

import pathlib

from twin_builder import RFP_DERIVED_BASES, visible_relationships

ROOT = pathlib.Path(__file__).resolve().parents[1]


def test_rfp_derived_relationships_are_dropped():
    rels = [
        {"role": "Consultant", "name": "Stale Advisors", "basis": "rfp_awarded"},
        {"role": "Actuary", "name": "Old Actuary Co", "basis": "rfp_incumbent"},
    ]
    assert visible_relationships(rels) == []


def test_live_relationships_survive():
    """The governance facet is a mixture; only the frozen half is withheld.

    ips_relationship and actuary_relationship are appended from live IPS and
    CAFR extraction, so hiding the whole facet would discard good data.
    """
    live = [
        {"role": "Consultant", "name": "Named In Current IPS", "basis": "ips_declared"},
        {"role": "Actuary", "name": "From FY2024 CAFR", "basis": "cafr_actuarial"},
    ]
    assert visible_relationships(live) == live


def test_mixed_list_keeps_only_the_live_entries():
    mixed = [
        {"role": "Consultant", "name": "Stale", "basis": "rfp_awarded"},
        {"role": "Actuary", "name": "Fresh", "basis": "cafr_actuarial"},
        {"role": "Custodian", "name": "Also stale", "basis": "rfp_incumbent"},
    ]
    assert [r["name"] for r in visible_relationships(mixed)] == ["Fresh"]


def test_relationship_without_a_basis_is_kept():
    """Unknown provenance is not evidence of staleness — do not silently drop."""
    assert visible_relationships([{"role": "Legal", "name": "X"}]) == \
        [{"role": "Legal", "name": "X"}]


def test_the_rfp_state_facet_is_not_rendered():
    """The 'RFP / search state' expander is gone from the twin page."""
    app_src = (ROOT / "app.py").read_text(encoding="utf-8")
    # Matches the call, not the phrase — the removal comment names it too.
    assert 'st.expander("RFP / search state"' not in app_src, (
        "the RFP expander is back — rfp_records is still frozen")
    assert 'f["rfp_state"]' not in app_src, (
        "app.py reads the frozen rfp_state facet for display")


def test_the_facet_is_still_built_and_stored():
    """Hiding is a display decision. The data stays in the snapshot."""
    import twin_builder
    src = pathlib.Path(twin_builder.__file__).read_text(encoding="utf-8")
    assert '"rfp_state": rfp_state' in src, (
        "rfp_state must still be built — build_manager_roster and any future "
        "refresh depend on the pipeline staying intact")
    assert RFP_DERIVED_BASES == ("rfp_awarded", "rfp_incumbent")
