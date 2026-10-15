"""upsert_plan keeps the plans table in line with data/known_plans.json."""
from __future__ import annotations

from database import Plan, get_session, upsert_plan


def _entry(**over):
    entry = {"id": "xyz", "name": "XYZ Retirement", "abbreviation": "XYZ",
             "state": "CA", "aum_billions": 10.0, "website": "https://old.org",
             "materials_url": "https://old.org/board", "fiscal_year_end": "06-30",
             "doc_types": ["agenda"], "cafr_url": "https://old.org/cafr.pdf"}
    entry.update(over)
    return entry


def test_inserts_a_new_plan_with_only_table_columns(tmp_db):
    s = get_session()
    upsert_plan(s, _entry())
    s.commit()
    p = s.get(Plan, "xyz")
    assert (p.name, p.website, p.fiscal_year_end) == (
        "XYZ Retirement", "https://old.org", "06-30")


def test_registry_changes_reach_an_existing_row(tmp_db):
    s = get_session()
    upsert_plan(s, _entry())
    s.commit()

    upsert_plan(s, _entry(fiscal_year_end="12-31", website="https://new.gov",
                          aum_billions=12.5))
    s.commit()
    p = s.get(Plan, "xyz")
    assert (p.fiscal_year_end, p.website, p.aum_billions) == (
        "12-31", "https://new.gov", 12.5)


def test_a_key_absent_from_the_entry_leaves_the_column_alone(tmp_db):
    s = get_session()
    upsert_plan(s, _entry())
    s.commit()

    partial = _entry(materials_url="https://old.org/meetings")
    del partial["website"]
    upsert_plan(s, partial)
    s.commit()
    p = s.get(Plan, "xyz")
    assert (p.website, p.materials_url) == (
        "https://old.org", "https://old.org/meetings")
