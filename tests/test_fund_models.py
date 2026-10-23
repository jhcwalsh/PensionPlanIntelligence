"""The fund-crowding tables exist after init_db() and accept manager-only rows."""
from sqlalchemy import inspect

import database
from database import FundCommitment, Plan


def test_fund_tables_created():
    names = set(inspect(database.engine).get_table_names())
    assert {"fund_action_extracts", "funds", "fund_commitments"} <= names


def test_manager_only_rows_have_no_fund():
    s = database.get_session()
    s.add(Plan(id="p1", name="P1"))
    s.add_all([
        FundCommitment(plan_id="p1", fund_id=None, manager_canonical="almanac",
                       status="approved", match_confidence="manager_only"),
        FundCommitment(plan_id="p1", fund_id=None, manager_canonical="crow",
                       status="approved", match_confidence="manager_only"),
    ])
    s.commit()
    assert s.query(FundCommitment).count() == 2
    s.close()
