"""Label collection, defensive canonicalization, and mock-mode classify."""
import json

from database import CafrAllocation, CafrExtract, Document, Plan, get_session
import twin_builder
from scripts import normalize_asset_classes as nac


def test_collect_distinct_labels(tmp_db):
    session = get_session()
    session.add(Plan(id="p1", name="P", abbreviation="P", state="CA"))
    doc = Document(plan_id="p1", url="https://x/c.pdf", filename="c.pdf",
                   doc_type="cafr", extraction_status="done", fiscal_year=2025)
    session.add(doc)
    session.commit()
    ext = CafrExtract(plan_id="p1", document_id=doc.id, fiscal_year=2025)
    session.add(ext)
    session.commit()
    session.add_all([
        CafrAllocation(cafr_extract_id=ext.id, asset_class="Global Equity"),
        CafrAllocation(cafr_extract_id=ext.id, asset_class="Global Equity"),
        CafrAllocation(cafr_extract_id=ext.id, asset_class="Private Credit "),
    ])
    session.commit()
    labels = nac.collect_distinct_labels(session)
    assert labels.count("Global Equity") == 1
    assert "Private Credit" in labels  # stripped
    session.close()


def test_canonical_asset_class_defensive(tmp_path, monkeypatch):
    mappings = {"Global Equity": "public_equity_global", "Weird": "not_a_real_class"}
    assert twin_builder.canonical_asset_class("Global Equity", mappings) == "public_equity_global"
    assert twin_builder.canonical_asset_class("Never Seen", mappings) == "unmapped"
    assert twin_builder.canonical_asset_class("Weird", mappings) == "unmapped"
    assert twin_builder.canonical_asset_class(None, mappings) == "unmapped"


def test_classify_batch_mock(monkeypatch):
    monkeypatch.setenv("LLM_MODE", "mock")
    out = nac._classify_batch(None, ["Global Equity"])
    assert out == {"Global Equity": {"canonical": "unmapped", "confidence": "low"}}
