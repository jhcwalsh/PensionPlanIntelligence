"""Stale figures, and the picker crash that came from filtering the options.

Two faults reported together on 2026-09-01, both about the Period end column
added the same day.

1. Selecting 2026Q2 and then switching asset class crashed the page. The
   picker's options were derived from the selected class's own rows, and
   Streamlit raises when a widget's stored selection is missing from the
   options it is handed. Cash has no 2026Q2 reading, so that move was enough.

2. Both tables claim to be current -- one says "Latest return by asset class"
   in its title -- and the corpus reaches back to 1994Q4. A 2014 return in a
   column beside a 2026 one reads as this year's number.
"""
from datetime import date

import pytest

import database
import queries
from database import (Plan, PlanAssetClassHorizon, PlanAssetClassPerformance)


@pytest.fixture
def session(tmp_db):
    s = database.get_session()
    s.add_all([Plan(id="mcera", name="MCERA", state="CA"),
               Plan(id="other", name="Other Plan", state="TX")])
    s.commit()
    yield s
    s.close()


def _horizon(session, plan_id, asset_class, horizon_key, pct, period_label):
    session.add(PlanAssetClassHorizon(
        plan_id=plan_id, asset_class=asset_class, horizon_key=horizon_key,
        return_pct=pct, period_label=period_label,
        as_of_date=date(2026, 5, 14), source="board_doc"))


def _collated(session, plan_id, asset_class, pct, period_label):
    session.add(PlanAssetClassPerformance(
        plan_id=plan_id, asset_class=asset_class, return_pct=pct,
        period_label=period_label, horizon="annual",
        as_of_date=date(2026, 5, 14), source="board_doc"))


# --------------------------------------------------------------------------
# The cutoff
# --------------------------------------------------------------------------

def test_the_cutoff_is_a_quarter_string_so_it_sorts():
    """Compared with <, not parsed. Quarter labels are built to sort."""
    assert queries.EARLIEST_PERIOD_END == "2025Q1"
    assert queries._too_old("2024Q4")
    assert queries._too_old("1994Q4")
    assert not queries._too_old("2025Q1")
    assert not queries._too_old("2026Q2")


def test_an_undated_figure_is_not_treated_as_old():
    """Missing information, not evidence of age. Dropping it would hide a
    plan for a reason the reader cannot see on screen."""
    assert not queries._too_old(None)
    assert not queries._too_old("")


def test_a_stale_collated_row_is_dropped_whole(session):
    """Every figure in one of these rows shares a period, so age is a property
    of the row."""
    _collated(session, "mcera", "real_estate", 6.0, "FY2025")     # 2025Q2
    _collated(session, "other", "real_estate", 9.9, "FY2014")     # 2014Q2
    session.commit()

    plans = {r["Plan"] for r in queries.collated_performance_rows(session)}
    assert plans == {"MCERA"}


def test_a_stale_horizon_cell_goes_without_taking_its_row(session):
    """Age is per *cell* here. A plan's ten-year figure can be four years
    older than its one-year figure, and dropping the row would discard a
    current number to get rid of a stale one."""
    _horizon(session, "mcera", "real_estate", "annual", 6.0, "1 Year")
    _horizon(session, "mcera", "real_estate", "10y", 7.0, "10 Year (Q3 2014)")
    session.commit()

    row = queries.asset_class_horizon_rows(session, "real_estate")[0]
    assert row["1 year"] == 6.0
    assert "10 year" not in row or row["10 year"] is None
    assert row["Period end"] == "2026Q1"


def test_a_plan_whose_every_cell_is_stale_disappears(session):
    """Rather than becoming an all-blank row, which reads as a plan that
    reported and did badly."""
    _horizon(session, "mcera", "real_estate", "annual", 6.0, "1 Year")
    _horizon(session, "other", "real_estate", "annual", 9.9, "FY2014")
    session.commit()

    plans = {r["Plan"] for r in queries.asset_class_horizon_rows(session, "real_estate")}
    assert plans == {"MCERA"}


# --------------------------------------------------------------------------
# The picker options -- the actual crash
# --------------------------------------------------------------------------

def test_the_quarter_options_do_not_depend_on_asset_class(session):
    """The crash, in one assertion.

    real_estate has a 2026Q2 reading and cash does not. Options derived from
    the selected class would drop 2026Q2 on the switch to cash, and Streamlit
    raises when session state holds an option it was not offered.
    """
    _horizon(session, "mcera", "real_estate", "quarter", 1.0, "Q2 2026")
    _horizon(session, "other", "cash_short_term", "annual", 2.0, "Q1 2026")
    session.commit()

    options = queries.asset_class_horizon_quarters(session)
    assert "2026Q2" in options
    assert "2026Q1" in options

    # And the class that lacks it is still queryable without error.
    assert queries.asset_class_horizon_rows(session, "cash_short_term")


def test_quarter_options_are_newest_first(session):
    _horizon(session, "mcera", "real_estate", "quarter", 1.0, "Q1 2026")
    _horizon(session, "mcera", "real_estate", "annual", 2.0, "Q4 2025")
    session.commit()

    assert queries.asset_class_horizon_quarters(session) == ["2026Q1", "2025Q4"]


def test_quarter_options_respect_the_cutoff(session):
    """Otherwise the picker offers a quarter that filters to nothing, every
    time."""
    _horizon(session, "mcera", "real_estate", "annual", 1.0, "Q1 2026")
    _horizon(session, "other", "real_estate", "annual", 2.0, "Q3 2014")
    session.commit()

    assert queries.asset_class_horizon_quarters(session) == ["2026Q1"]


def test_quarter_options_on_an_empty_table_is_an_empty_list(session):
    assert queries.asset_class_horizon_quarters(session) == []
