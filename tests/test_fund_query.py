"""who_else_is_in / funds_held_alongside (funds/query.py)."""
import json
from datetime import date

import pytest

import database
from database import Fund, FundCommitment, Plan
from funds.query import (fund_detail, funds_held_alongside, most_crowded, overview, resolve_fund,
                         who_else_is_in)

TODAY = date(2026, 10, 22)


@pytest.fixture
def s():
    sess = database.get_session()
    for pid, aum in (("a", 100.0), ("b", 50.0), ("c", None), ("d", 20.0)):
        sess.add(Plan(id=pid, name=pid.upper(), aum_billions=aum))
    sess.commit()
    yield sess
    sess.close()


def _fund(s, name, manager, series, number, key=None):
    f = Fund(fund_key=key or f"{manager.lower()}|{series}|{number or ''}", canonical_name=name,
             manager_canonical=manager, series=series, number=number)
    s.add(f)
    s.flush()
    return f


def _hold(s, plan, fund, status="approved", amount=100.0, first="2026-03-01", last=None,
          confidence="exact", evidence=None):
    last = last or first
    if evidence is None:
        evidence = [{"document_id": i, "meeting_date": d, "status": status}
                    for i, d in enumerate(sorted({first, last} - {None}) or [None])]
    s.add(FundCommitment(plan_id=plan, fund_id=fund.id if fund else None,
                         manager_canonical=fund.manager_canonical if fund else None,
                         fund_name_raw=fund.canonical_name if fund else None,
                         status=status, amount_millions=amount, first_seen=first,
                         last_seen=last, evidence=json.dumps(evidence), match_confidence=confidence))
    s.commit()


def test_holders_exclude_asking_plan_and_pending_split(s):
    f = _fund(s, "GTCR Fund XV", "GTCR", "", 15)
    _hold(s, "a", f)
    _hold(s, "b", f)
    _hold(s, "d", f, status="proposed")
    r = who_else_is_in("GTCR Fund XV", exclude_plan="a", session=s, today=TODAY)
    assert [h["plan_id"] for h in r["holders"]] == ["b"]
    assert [p["plan_id"] for p in r["pending"]] == ["d"]
    assert r["fund"]["name"] == "GTCR Fund XV"


def test_resolve_ambiguous_returns_candidates(s):
    _fund(s, "KKR North America Fund XIII", "KKR", "north america", 13)
    f14 = _fund(s, "KKR North America Fund XIV", "KKR", "north america", 14)
    _hold(s, "b", f14)
    r = who_else_is_in("KKR North America", session=s, today=TODAY)
    assert r["fund"] is None
    assert len(r["candidates"]) == 2
    assert r["holders"] == []


def test_resolve_roman_and_arabic_equivalent(s):
    f14 = _fund(s, "KKR North America Fund XIV", "KKR", "north america", 14)
    fund, _ = resolve_fund(s, "KKR North America Fund 14")
    assert fund is not None and fund.id == f14.id


def test_other_vintages_separate(s):
    f13 = _fund(s, "KKR North America Fund XIII", "KKR", "north america", 13)
    f14 = _fund(s, "KKR North America Fund XIV", "KKR", "north america", 14)
    _hold(s, "b", f13)
    r = who_else_is_in("KKR North America Fund XIV", session=s, today=TODAY)
    assert r["holders"] == []
    fam = r["same_family_other_vintages"]
    assert [v["fund"]["number"] for v in fam] == [13]
    assert [h["plan_id"] for h in fam[0]["holders"]] == ["b"]


def test_unnumbered_window_anchored(s):
    f = _fund(s, "Wellington Emerging Markets Fund", "Wellington", "emerging markets", None)
    _hold(s, "a", f, first="2026-06-01")
    _hold(s, "b", f, first="2025-07-01")
    _hold(s, "c", f, first="2025-01-01")
    _hold(s, "d", f, first=None, last=None)
    s.add(Plan(id="e", name="E", aum_billions=1.0))
    s.commit()
    _hold(s, "e", f, first="2027-05-01")
    r = who_else_is_in("Wellington Emerging Markets Fund", exclude_plan="a", session=s, today=TODAY)
    assert sorted(h["plan_id"] for h in r["holders"]) == ["b", "e"]
    assert [(p["plan_id"], p.get("reason")) for p in r["pending"]] == [("d", "undated")]


def test_unnumbered_anchor_defaults_to_today(s):
    f = _fund(s, "Wellington Emerging Markets Fund", "Wellington", "emerging markets", None)
    _hold(s, "b", f, first="2026-01-01")
    _hold(s, "c", f, first="2025-06-01")
    r = who_else_is_in("Wellington Emerging Markets Fund", session=s, today=TODAY)
    assert [h["plan_id"] for h in r["holders"]] == ["b"]


def test_holder_pct_null_when_aum_missing(s):
    f = _fund(s, "GTCR Fund XV", "GTCR", "", 15)
    _hold(s, "b", f, amount=100.0)
    _hold(s, "c", f, amount=100.0)
    r = who_else_is_in("GTCR Fund XV", session=s, today=TODAY)
    pct = {h["plan_id"]: h["pct_of_plan_aum"] for h in r["holders"]}
    assert pct == {"b": 0.2, "c": None}


def test_pipeline_and_declined_never_returned(s):
    f = _fund(s, "GTCR Fund XV", "GTCR", "", 15)
    _hold(s, "b", f, status="pipeline")
    _hold(s, "c", f, status="declined")
    r = who_else_is_in("GTCR Fund XV", session=s, today=TODAY)
    assert r["holders"] == [] and r["pending"] == []


def test_manager_only_mentions(s):
    f = _fund(s, "Almanac Realty Securities IX", "Almanac", "realty securities", 9)
    _hold(s, "b", None)
    s.query(FundCommitment).filter_by(plan_id="b").update(
        {"manager_canonical": "Almanac", "match_confidence": "manager_only"})
    s.commit()
    r = who_else_is_in("Almanac Realty Securities IX", session=s, today=TODAY)
    assert [m["plan_id"] for m in r["manager_only_mentions"]] == ["b"]


def test_funds_held_alongside_counts_cooccurrence(s):
    x = _fund(s, "X Fund I", "X", "", 1)
    y = _fund(s, "Y Fund II", "Y", "", 2)
    z = _fund(s, "Z Fund III", "Z", "", 3)
    for p in ("a", "b", "c"):
        _hold(s, p, x)
    _hold(s, "a", y)
    _hold(s, "b", y)
    _hold(s, "c", z)
    r = funds_held_alongside("X Fund I", session=s, today=TODAY)
    assert r["holders_considered"] == 3
    assert [(a["fund"]["name"], a["count"], a["share_of_holders"]) for a in r["alongside"]] == [("Y Fund II", 2, 0.667)]


def test_coverage_block_present_and_counts_plans(s):
    f = _fund(s, "GTCR Fund XV", "GTCR", "", 15)
    _hold(s, "a", f, first="2025-02-01")
    _hold(s, "b", f, first="2026-09-30")
    cov = who_else_is_in("Nothing Like This", session=s, today=TODAY)["coverage"]
    assert cov["plans_searched"] == 2
    assert (cov["earliest_meeting"], cov["latest_meeting"]) == ("2025-02-01", "2026-09-30")
    assert "not evidence" in cov["caveat"]


def test_partial_name_returns_candidates_not_a_guess(s):
    _fund(s, "Ares Centre Street Pathfinder Core Plus", "Ares", "centre street pathfinder core plus", None)
    _fund(s, "Blackstone Real Estate Partners X", "Blackstone", "real estate partners", 10)
    for q in ("Ares Pathfinder", "Blackstone X"):
        fund, cands = resolve_fund(s, q)
        assert fund is None and len(cands) == 1, q


def test_unnumbered_window_checks_each_mention_not_the_span(s):
    f = _fund(s, "Wellington Emerging Markets Fund", "Wellington", "emerging markets", None)
    _hold(s, "a", f, first="2024-07-01")
    _hold(s, "b", f, first="2023-01-15", last="2026-01-15")
    r = who_else_is_in("Wellington Emerging Markets Fund", exclude_plan="a", session=s, today=TODAY)
    assert r["holders"] == []


def test_exact_name_beats_co_invest_vehicles(s):
    main = _fund(s, "Francisco Partners Fund VIII", "Francisco Partners", "", 8)
    _fund(s, "FP VIII PA Co-Invest, L.P.", "Francisco Partners", "fp # pa co-invest", 8)
    fund, _ = resolve_fund(s, "Francisco Partners VIII")
    assert fund is not None and fund.id == main.id


def test_most_crowded_ranks_by_approved_holders(s):
    f1 = _fund(s, "GTCR Fund XV", "GTCR", "", 15)
    f2 = _fund(s, "KKR North America Fund XIV", "KKR", "north america", 14)
    lone = _fund(s, "Lone Fund I", "Lone", "", 1)
    for p in ("a", "b", "c"):
        _hold(s, p, f2, amount=100.0)
    _hold(s, "a", f1, amount=50.0)
    _hold(s, "b", f1, amount=None)
    _hold(s, "d", f1, status="proposed")
    _hold(s, "d", f2, status="pipeline")
    _hold(s, "a", lone)
    rows = most_crowded(session=s, today=TODAY)
    assert [(r["fund"]["name"], r["holders"]) for r in rows] == [
        ("KKR North America Fund XIV", 3), ("GTCR Fund XV", 2)]
    assert rows[0]["total_millions"] == 300.0
    assert rows[1]["total_millions"] == 50.0
    assert rows[1]["plans"] == ["A", "B"]
    assert rows[1]["fund"]["key"] == f1.fund_key


def test_most_crowded_applies_the_unnumbered_window(s):
    f = _fund(s, "Wellington EM Fund", "Wellington", "emerging markets", None)
    _hold(s, "a", f, first="2026-06-01")
    _hold(s, "b", f, first="2024-01-15")
    _hold(s, "c", f, first="2026-09-01")
    rows = most_crowded(session=s, today=TODAY)
    assert [(r["fund"]["name"], r["holders"]) for r in rows] == [("Wellington EM Fund", 2)]
    assert rows[0]["holders"] == len(who_else_is_in(f.fund_key, session=s, today=TODAY)["holders"])


def test_most_crowded_filters_by_asset_class(s):
    pe = _fund(s, "GTCR Fund XV", "GTCR", "", 15)
    infra = _fund(s, "ECP VI", "ECP", "", 6)
    pe.asset_class, infra.asset_class = "private_equity", "real_assets_infrastructure"
    for p in ("a", "b"):
        _hold(s, p, pe)
        _hold(s, p, infra)
    rows = most_crowded(asset_class="private_equity", session=s, today=TODAY)
    assert [r["fund"]["name"] for r in rows] == ["GTCR Fund XV"]


def test_overview_counts_and_indexes_funds(s):
    f = _fund(s, "GTCR Fund XV", "GTCR", "", 15)
    _fund(s, "Apex Fund I", "Apex", "", 1)
    _hold(s, "a", f)
    _hold(s, "b", f)
    ov = overview(session=s, today=TODAY)
    assert ov["funds"] == 2 and ov["crowded"] == 1
    assert list(ov["fund_index"].values()) == ["Apex Fund I", "GTCR Fund XV"]
    assert ov["coverage"]["plans_searched"] == 2


def test_overview_carries_the_ranked_crowded_funds(s):
    f = _fund(s, "GTCR Fund XV", "GTCR", "", 15)
    g = _fund(s, "TPG Partners X", "TPG", "partners", 10)
    for p in ("a", "b", "d"):
        _hold(s, p, f)
    for p in ("a", "b"):
        _hold(s, p, g)
    ov = overview(session=s, today=TODAY)
    assert ov["crowded_funds"] == most_crowded(limit=10**9, session=s, today=TODAY)
    assert [r["fund"]["name"] for r in ov["crowded_funds"]] == ["GTCR Fund XV", "TPG Partners X"]


def _family(s, vintages):
    funds = [_fund(s, f"KKR North America Fund {n}", "KKR", "north america", n) for n in vintages]
    for f in funds:
        _hold(s, "a", f)
        _hold(s, "b", f)
    x = _fund(s, "X Fund I", "X", "", 1)
    _hold(s, "a", x)
    _hold(s, "b", x)
    _hold(s, "c", None)  # unrelated manager-only row
    mgr = FundCommitment(plan_id="d", fund_id=None, manager_canonical="KKR", status="approved",
                         evidence="[]", match_confidence="manager_only")
    s.add(mgr)
    s.commit()
    return funds[-1]


def test_fund_detail_matches_the_separate_calls(s):
    f = _family(s, [12, 13, 14])
    d = fund_detail(f.fund_key, session=s, today=TODAY, alongside_limit=8)
    assert d["who"] == who_else_is_in(f.fund_key, session=s, today=TODAY)
    assert d["alongside"] == funds_held_alongside(f.fund_key, session=s, today=TODAY, limit=8)
    assert [v["fund"]["number"] for v in d["who"]["same_family_other_vintages"]] == [13, 12]
    assert len(d["who"]["manager_only_mentions"]) == 1


def test_fund_detail_query_count_does_not_grow_with_vintages(s):
    from sqlalchemy import event
    f = _family(s, [1, 2])

    def queries():
        n = [0]
        listener = lambda *a, **k: n.__setitem__(0, n[0] + 1)
        event.listen(database.engine, "before_cursor_execute", listener)
        try:
            fund_detail(f.fund_key, session=s, today=TODAY)
        finally:
            event.remove(database.engine, "before_cursor_execute", listener)
        return n[0]

    few = queries()
    for n in (3, 4, 5, 6):
        _hold(s, "a", _fund(s, f"KKR North America Fund {n}", "KKR", "north america", n))
    assert queries() == few
