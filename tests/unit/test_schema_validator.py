"""Schema validator tests — proves lib/rfp_schema.json loads and rejects bad inputs."""

from __future__ import annotations

from copy import deepcopy

import pytest

from lib.schema_validator import is_valid, load_schema, validate_record
from rfp.ids import compute_rfp_id


def _good_record() -> dict:
    return {
        "rfp_id": compute_rfp_id("calpers", "Consultant", "2024-03-15",
                                 "General Investment Consultant"),
        "plan_id": "calpers",
        "rfp_type": "Consultant",
        "title": "General Investment Consultant",
        "status": "Issued",
        "release_date": "2024-03-15",
        "response_due_date": "2024-05-01",
        "award_date": None,
        "mandate_size_usd_millions": None,
        "asset_class": None,
        "incumbent_manager": "Wilshire Associates",
        "incumbent_manager_id": None,
        "shortlisted_managers": [],
        "awarded_manager": None,
        "source_document": {
            "url": "https://www.calpers.ca.gov/board/2024-03/packet.pdf",
            "page_number": 42,
            "document_id": 1234,
        },
        "source_quote": "Staff recommends issuing an RFP for general investment consulting services in March 2024.",
        "extraction_confidence": 0.92,
    }


def test_schema_loads():
    schema = load_schema()
    assert schema["$id"].endswith("rfp_v1.json")
    assert "rfp_id" in schema["required"]


def test_good_record_passes():
    assert is_valid(_good_record()), validate_record(_good_record())


def test_missing_required_field_fails():
    record = _good_record()
    del record["plan_id"]
    errors = validate_record(record)
    assert errors
    assert any("plan_id" in e for e in errors)


def test_bad_rfp_type_fails():
    record = _good_record()
    record["rfp_type"] = "Marketing"
    errors = validate_record(record)
    assert errors
    assert any("rfp_type" in e for e in errors)


def test_bad_rfp_id_format_fails():
    record = _good_record()
    record["rfp_id"] = "not-hex-at-all-uppercase"
    errors = validate_record(record)
    assert errors


def test_confidence_out_of_range_fails():
    record = _good_record()
    record["extraction_confidence"] = 1.5
    errors = validate_record(record)
    assert errors


def test_short_source_quote_fails():
    record = _good_record()
    record["source_quote"] = "too short"
    errors = validate_record(record)
    assert errors


def test_unknown_property_rejected():
    record = _good_record()
    record["unknown_field"] = "nope"
    errors = validate_record(record)
    assert errors


def test_incumbent_manager_id_must_be_null():
    record = _good_record()
    record["incumbent_manager_id"] = "wilshire-associates"
    errors = validate_record(record)
    assert errors


def test_null_dates_allowed():
    record = _good_record()
    record["release_date"] = None
    record["response_due_date"] = None
    assert is_valid(record), validate_record(record)


@pytest.mark.parametrize("status", ["Planned", "Issued", "ResponsesReceived",
                                     "FinalistsNamed", "Awarded", "Withdrawn"])
def test_all_status_values_accepted(status):
    record = _good_record()
    record["status"] = status
    assert is_valid(record), validate_record(record)
