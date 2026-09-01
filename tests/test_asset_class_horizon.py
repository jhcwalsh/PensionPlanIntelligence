"""The per-asset-class view: one asset class, every plan, every horizon.

Mirrors the existing Performance tab (one plan across asset classes) the
other way around -- one asset class across plans, quarter through 10-year
side by side. Covers the new PlanAssetClassHorizon table, the builder logic
that populates it (scripts/build_performance_view.py), and the read
(queries.asset_class_horizon_rows).

Unlike plan_asset_class_performance's pick_latest, which keeps at most two
rows per plan from a single document, this table keeps the *best available*
reading per (plan, asset_class, horizon_key) cell independently -- see
pick_best_per_cell's docstring for why that does not contradict pick_latest's
single-document rule.
"""
import json
from datetime import date

import pytest

import database
from database import (Document, DocumentSectionRead, Plan,
                      PlanAssetClassHorizon, Summary)
from scripts import build_performance_view as bpv

import queries


@pytest.fixture
def session(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA"))
    s.commit()
    yield s
    s.close()


def _doc(session, plan_id="mcera", when=date(2026, 8, 26), name="pack.pdf"):
    d = Document(plan_id=plan_id, url=f"https://x/{name}", filename=name,
                meeting_date=when, extraction_status="done")
    session.add(d)
    session.commit()
    return d


def _payload(cls, pct, period):
    return json.dumps([{"asset_class": cls, "return_pct": pct,
                        "period": period}])


# --------------------------------------------------------------------------
# pick_best_per_cell -- the selection rule
# --------------------------------------------------------------------------

def _row(plan_id="mcera", asset_class="real_estate", return_pct=1.0,
         period_label="3-Year", as_of=date(2026, 1, 1), source="board_doc",
         document_id=1):
    return {"plan_id": plan_id, "asset_class": asset_class,
            "return_pct": return_pct, "period_label": period_label,
            "horizon": bpv.horizon_of(period_label), "as_of_date": as_of,
            "source": source, "document_id": document_id}


def test_the_newer_reading_wins_the_cell():
    """Within one quarter. Since period_end joined the key, two readings of
    *different* quarters are two cells and both survive -- that is
    test_two_quarters_of_the_same_cell_both_survive below. This is the
    narrower rule that still holds: same period, later paper wins.

    Both dates sit in 2025Q4 (the label states no period, so the quarter comes
    from the document date rounded back).
    """
    older = _row(return_pct=5.0, as_of=date(2026, 1, 5), document_id=1)
    newer = _row(return_pct=7.0, as_of=date(2026, 2, 20), document_id=2)
    out = bpv.pick_best_per_cell([older, newer])
    assert {r["period_end"] for r in out} == {"2025Q4"}
    assert len(out) == 1
    assert out[0]["return_pct"] == 7.0
    assert out[0]["document_id"] == 2


def test_a_tie_on_date_breaks_toward_targeted_read_then_cafr():
    same_date = date(2026, 1, 1)
    board = _row(return_pct=1.0, as_of=same_date, source="board_doc", document_id=1)
    cafr = _row(return_pct=2.0, as_of=same_date, source="cafr", document_id=2)
    targeted = _row(return_pct=3.0, as_of=same_date, source="targeted_read", document_id=3)

    out = bpv.pick_best_per_cell([board, cafr])
    assert out[0]["source"] == "cafr" and out[0]["return_pct"] == 2.0

    out = bpv.pick_best_per_cell([cafr, targeted, board])
    assert out[0]["source"] == "targeted_read" and out[0]["return_pct"] == 3.0


def test_an_unclear_label_contributes_nothing_to_the_cell_table():
    unclear = _row(period_label="12/31/24")
    clear = _row(period_label="3-Year", return_pct=9.0, as_of=date(2020, 1, 1))
    out = bpv.pick_best_per_cell([unclear, clear])
    assert len(out) == 1
    assert out[0]["return_pct"] == 9.0


def test_different_horizons_are_different_cells_even_same_plan_and_class():
    """This is the whole point of the table: unlike pick_latest's two-row
    cap, a plan can have many cells here -- quarter, annual, 3y, 5y, 10y --
    each independently sourced."""
    rows = [
        _row(period_label="Q1 2026", return_pct=1.1, document_id=1),
        _row(period_label="1 Year", return_pct=2.2, document_id=2),
        _row(period_label="3-Year", return_pct=3.3, document_id=3),
        _row(period_label="5-Year", return_pct=5.5, document_id=4),
        _row(period_label="10-Year", return_pct=10.1, document_id=5),
    ]
    out = bpv.pick_best_per_cell(rows)
    keys = {r["horizon_key"] for r in out}
    assert keys == {"quarter", "annual", "3y", "5y", "10y"}
    assert len(out) == 5


def test_different_plans_and_classes_do_not_collide():
    rows = [
        _row(plan_id="mcera", asset_class="real_estate", return_pct=1.0),
        _row(plan_id="other", asset_class="real_estate", return_pct=2.0),
        _row(plan_id="mcera", asset_class="private_equity", return_pct=3.0),
    ]
    out = bpv.pick_best_per_cell(rows)
    assert len(out) == 3


# --------------------------------------------------------------------------
# The builder writes both tables in one run
# --------------------------------------------------------------------------

def test_main_populates_the_horizon_table_alongside_the_existing_view(
        session, monkeypatch):
    d = _doc(session)
    session.add(DocumentSectionRead(
        document_id=d.id, offset=1,
        returns_json=_payload("Real Estate", 8.1, "3-Year")))
    session.commit()

    monkeypatch.setattr("sys.argv", ["build_performance_view"])
    bpv.main()

    rows = session.query(PlanAssetClassHorizon).all()
    assert len(rows) == 1
    assert rows[0].plan_id == "mcera"
    assert rows[0].asset_class == "real_estate"
    assert rows[0].horizon_key == "3y"
    assert float(rows[0].return_pct) == 8.1


def test_a_plan_with_only_an_annual_figure_gets_one_horizon_row(session, monkeypatch):
    d = _doc(session)
    session.add(Summary(document_id=d.id,
                        performance_data=_payload("Real Estate", 4.4, "FY2025")))
    session.commit()

    monkeypatch.setattr("sys.argv", ["build_performance_view"])
    bpv.main()

    rows = session.query(PlanAssetClassHorizon).all()
    assert [r.horizon_key for r in rows] == ["annual"]


def test_horizon_table_survives_a_rebuild_without_duplicate_key_errors(
        session, monkeypatch):
    """Rebuilt wholesale: the DELETE clears it, so re-running the build must
    not hit the unique constraint on (plan_id, asset_class, horizon_key)."""
    d = _doc(session)
    session.add(DocumentSectionRead(
        document_id=d.id, offset=1,
        returns_json=_payload("Real Estate", 8.1, "3-Year")))
    session.commit()

    monkeypatch.setattr("sys.argv", ["build_performance_view"])
    bpv.main()
    bpv.main()

    rows = session.query(PlanAssetClassHorizon).all()
    assert len(rows) == 1


# --------------------------------------------------------------------------
# queries.asset_class_horizon_rows
# --------------------------------------------------------------------------

def _seed_horizon(session, plan_id, asset_class, horizon_key, return_pct,
                  as_of_date, document_id=None, period_label=None):
    """period_end is derived here exactly as pick_best_per_cell derives it.

    It is part of this table's key and the read layer trusts the stored value
    rather than recomputing, so a fixture that left it NULL would be seeding a
    row the builder cannot produce -- and the tests would pass against data
    that cannot exist.
    """
    session.add(PlanAssetClassHorizon(
        plan_id=plan_id, asset_class=asset_class, horizon_key=horizon_key,
        return_pct=return_pct, period_label=period_label,
        period_end=queries.period_end_quarter(period_label, as_of_date),
        as_of_date=as_of_date, source="board_doc", document_id=document_id))


def test_asset_class_horizon_rows_shape_and_columns(session):
    session.add(Plan(id="other", name="Other Plan", state="TX"))
    d1 = _doc(session, plan_id="mcera", name="a.pdf")
    d2 = _doc(session, plan_id="mcera", name="b.pdf", when=date(2026, 6, 1))
    session.commit()

    _seed_horizon(session, "mcera", "real_estate", "quarter", 1.5,
                 date(2026, 6, 1), document_id=d2.id)
    _seed_horizon(session, "mcera", "real_estate", "annual", 6.0,
                 date(2026, 8, 26), document_id=d1.id)
    # 2025-04-01 rather than 2025-01-01: with no period label the period end
    # falls back to the last quarter that had closed, and a 1 January date
    # resolves to 2024Q4 -- behind queries.EARLIEST_PERIOD_END, so the plan
    # would vanish and this test would be checking the cutoff by accident.
    _seed_horizon(session, "other", "real_estate", "3y", 9.0,
                 date(2025, 4, 1))
    session.commit()

    rows = queries.asset_class_horizon_rows(session, "real_estate")
    by_plan = {r["Plan"]: r for r in rows}

    mcera = by_plan["MCERA"]
    assert mcera["Quarter"] == 1.5
    assert mcera["1 year"] == 6.0
    assert mcera.get("3 year") is None
    assert mcera["Sources"] == 2
    assert mcera["As of"] == "2026-08-26"          # the newer of the two cells
    assert mcera["Document"] == "a.pdf"

    other = by_plan["Other Plan"]
    assert other["3 year"] == 9.0
    assert other.get("Quarter") is None
    assert other["Sources"] == 0                    # no document_id recorded


def test_asset_class_horizon_rows_only_returns_the_requested_class(session):
    _seed_horizon(session, "mcera", "real_estate", "annual", 4.0, date(2026, 1, 1))
    _seed_horizon(session, "mcera", "private_equity", "annual", 12.0, date(2026, 1, 1))
    session.commit()

    rows = queries.asset_class_horizon_rows(session, "real_estate")
    assert len(rows) == 1
    assert "1 year" in rows[0]
    assert rows[0]["1 year"] == 4.0


def test_asset_class_horizon_rows_sorts_by_one_year_descending_nones_last(session):
    session.add(Plan(id="b", name="B Plan", state="TX"))
    session.add(Plan(id="c", name="C Plan", state="NY"))
    session.commit()

    _seed_horizon(session, "mcera", "real_estate", "annual", 5.0, date(2026, 1, 1))
    _seed_horizon(session, "b", "real_estate", "annual", 9.0, date(2026, 1, 1))
    _seed_horizon(session, "c", "real_estate", "quarter", 1.0, date(2026, 1, 1))
    session.commit()

    rows = queries.asset_class_horizon_rows(session, "real_estate")
    plans_in_order = [r["Plan"] for r in rows]
    assert plans_in_order == ["B Plan", "MCERA", "C Plan"]


def test_each_cell_carries_its_own_period_end(session):
    """The reason _period_ends exists. A row mixes documents across columns,
    so one Period end per row would be a claim about cells it does not
    describe: here the 1-year figure is a 2026Q1 reading and the 10-year one
    is three quarters older.

    Both sit after queries.EARLIEST_PERIOD_END on purpose. A wider spread
    would demonstrate the same point and then be silently dropped by the
    staleness cutoff, testing that instead of this.
    """
    _seed_horizon(session, "mcera", "real_estate", "annual", 6.0,
                  date(2026, 5, 14), period_label="1 Year")
    _seed_horizon(session, "mcera", "real_estate", "10y", 7.0,
                  date(2025, 8, 10), period_label="10 Year")
    session.commit()

    row = queries.asset_class_horizon_rows(session, "real_estate")[0]

    assert row["_period_ends"] == {"1 year": "2026Q1", "10 year": "2025Q2"}
    # The row-level column says it spans, rather than picking one end.
    assert row["Period end"] == "2025Q2-2026Q1"


def test_a_row_whose_cells_agree_shows_one_quarter(session):
    _seed_horizon(session, "mcera", "real_estate", "annual", 6.0,
                  date(2026, 5, 14), period_label="1 Year")
    _seed_horizon(session, "mcera", "real_estate", "3y", 7.0,
                  date(2026, 4, 30), period_label="3 Year")
    session.commit()

    row = queries.asset_class_horizon_rows(session, "real_estate")[0]
    assert row["Period end"] == "2026Q1"


def test_the_stated_period_beats_the_document_date(session):
    """as_of_date is the *document's* date. A CAFR figure discussed at an
    August meeting is still a June figure, and filing it under 2026Q3 would
    put it in a bucket with the following year's quarters."""
    _seed_horizon(session, "mcera", "real_estate", "annual", 6.0,
                  date(2026, 8, 26), period_label="FY2025")
    session.commit()

    row = queries.asset_class_horizon_rows(session, "real_estate")[0]
    assert row["_period_ends"]["1 year"] == "2025Q2"


# --------------------------------------------------------------------------
# History: the table keeps a reading per quarter, not just the latest
# --------------------------------------------------------------------------

def test_two_quarters_of_the_same_cell_both_survive():
    """The change that made a 2025Q4 sweep possible.

    Keyed on (plan, class, horizon) alone, the 2026Q1 reading replaced the
    2025Q4 one and the plan simply vanished from any question about 2025Q4.
    26 plans reported a 2025Q4 private-equity figure; the view could show 17.
    """
    rows = [
        _row(period_label="1 Year", return_pct=5.0, as_of=date(2026, 2, 10),
             document_id=1),
        _row(period_label="1 Year", return_pct=7.0, as_of=date(2026, 5, 14),
             document_id=2),
    ]
    out = bpv.pick_best_per_cell(rows)

    assert {r["period_end"] for r in out} == {"2025Q4", "2026Q1"}
    assert {r["return_pct"] for r in out} == {5.0, 7.0}


def test_two_readings_of_the_SAME_quarter_still_collapse():
    """History, not duplication. A figure restated in a later pack supersedes
    the earlier printing of the same period -- which is why the date and
    source tie-breaks survive the key change."""
    rows = [
        _row(period_label="1 Year", return_pct=5.0, as_of=date(2026, 4, 2),
             document_id=1),
        _row(period_label="1 Year", return_pct=5.5, as_of=date(2026, 5, 14),
             document_id=2),
    ]
    out = bpv.pick_best_per_cell(rows)

    assert len(out) == 1
    assert out[0]["return_pct"] == 5.5          # the restatement wins
    assert out[0]["period_end"] == "2026Q1"     # same quarter throughout


def test_a_reading_with_no_resolvable_period_is_kept_under_a_null_quarter():
    """One cell, not a series. Dropping it would lose a figure the old key
    retained, and the standing rule here is not to discard data."""
    out = bpv.pick_best_per_cell([_row(period_label="3-Year", as_of=None)])
    assert len(out) == 1
    assert out[0]["period_end"] is None


def test_the_default_read_still_answers_how_are_they_doing_now(session):
    """With no quarter chosen the view must not stack every quarter a plan
    has ever reported into one cell."""
    d = _doc(session)
    for q, pct in (("Q4 2025", 5.0), ("Q1 2026", 7.0)):
        session.add(PlanAssetClassHorizon(
            plan_id="mcera", asset_class="real_estate", horizon_key="annual",
            return_pct=pct, period_label=q, period_end=bpv.queries.
            period_end_quarter(q, date(2026, 5, 14)),
            as_of_date=date(2026, 5, 14), source="board_doc",
            document_id=d.id))
    session.commit()

    rows = queries.asset_class_horizon_rows(session, "real_estate")
    assert len(rows) == 1
    assert rows[0]["1 year"] == 7.0             # the newer quarter
    assert rows[0]["Period end"] == "2026Q1"


def test_asking_for_an_older_quarter_returns_it(session):
    """The sweep. The plan has a newer reading and must still appear."""
    d = _doc(session)
    for q, pct in (("Q4 2025", 5.0), ("Q1 2026", 7.0)):
        session.add(PlanAssetClassHorizon(
            plan_id="mcera", asset_class="real_estate", horizon_key="annual",
            return_pct=pct, period_label=q, period_end=bpv.queries.
            period_end_quarter(q, date(2026, 5, 14)),
            as_of_date=date(2026, 5, 14), source="board_doc",
            document_id=d.id))
    session.commit()

    rows = queries.asset_class_horizon_rows(session, "real_estate", ["2025Q4"])
    assert len(rows) == 1
    assert rows[0]["1 year"] == 5.0
    assert rows[0]["Period end"] == "2025Q4"


def test_asset_class_horizons_ordering():
    assert queries.ASSET_CLASS_HORIZONS == (
        ("quarter", "Quarter"),
        ("annual", "1 year"),
        ("3y", "3 year"),
        ("5y", "5 year"),
        ("10y", "10 year"),
    )


def test_asset_class_horizon_rows_reuses_collated_classes_for_the_picker():
    assert queries.COLLATED_CLASSES  # sanity: still the shared list
    canon_keys = {k for k, _ in queries.COLLATED_CLASSES}
    assert "real_estate" in canon_keys
