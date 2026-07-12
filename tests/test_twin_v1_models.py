"""v1 tables exist, round-trip, and enforce their uniqueness contracts."""
import pytest
from sqlalchemy.exc import IntegrityError

from database import (
    ASSET_CLASS_CANONICAL, CafrActuarial, IpsAllocation, IpsExtract,
    PlanManagerRoster, get_session,
)


def test_taxonomy_constant():
    assert "private_equity" in ASSET_CLASS_CANONICAL
    assert "unmapped" in ASSET_CLASS_CANONICAL


def test_ips_extract_roundtrip(tmp_db):
    session = get_session()
    ext = IpsExtract(plan_id="p1", ips_document_id=1, target_return_pct=7.0,
                     effective_date="2025-01-01", objectives='{"a": 1}')
    session.add(ext)
    session.commit()
    session.add(IpsAllocation(ips_extract_id=ext.id, asset_class="Global Equity",
                              target_pct=40.0, range_low=35.0, range_high=45.0))
    session.commit()
    assert session.query(IpsAllocation).one().ips_extract_id == ext.id
    session.close()


def test_cafr_actuarial_unique_per_document(tmp_db):
    session = get_session()
    session.add(CafrActuarial(plan_id="p1", document_id=7, funded_ratio_pct=75.0))
    session.commit()
    session.add(CafrActuarial(plan_id="p1", document_id=7, funded_ratio_pct=80.0))
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback(); session.close()


def test_roster_unique_key(tmp_db):
    session = get_session()
    session.add(PlanManagerRoster(plan_id="p1", canonical_name="BlackRock",
                                  role="manager", status="current"))
    session.commit()
    session.add(PlanManagerRoster(plan_id="p1", canonical_name="BlackRock",
                                  role="manager", status="terminated"))
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback(); session.close()
