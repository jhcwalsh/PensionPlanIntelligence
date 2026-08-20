"""Label collection, defensive canonicalization, and mock-mode classify."""
import hashlib
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


def test_mappings_save_load_roundtrip_non_ascii(tmp_path, monkeypatch):
    """cp1252-default Windows writes must not poison the utf-8 reader.

    Regression: labels like "DoubleLine – Core Plus" (en-dash) written
    without an explicit encoding crashed every twin/roster build.
    """
    path = tmp_path / "asset_class_mappings.json"
    monkeypatch.setattr(nac, "MAPPINGS_PATH", path)
    nac._save({"DoubleLine – Core Plus": {"canonical": "fixed_income_core",
                                               "confidence": "high"}})
    raw = path.read_bytes()
    raw.decode("utf-8")  # must not raise
    assert nac._load_existing()["DoubleLine – Core Plus"]["canonical"] == "fixed_income_core"


def test_mock_run_cannot_touch_committed_mappings(tmp_db, monkeypatch, tmp_path):
    """A mock-mode run must leave data/asset_class_mappings.json byte-identical.

    Regression: ``_classify_batch`` returns a blanket ``unmapped``/``low``
    sentinel in mock mode, and ``main()`` only ever processes labels *absent*
    from the mapping — so a single stray mock run (LLM_MODE inherited from a
    previous pytest invocation, say) would permanently poison every unclassified
    label in the committed config, which the monthly GHA job then commits.
    """
    committed = nac._COMMITTED_MAPPINGS_PATH
    before = committed.read_bytes()
    digest_before = hashlib.sha256(before).hexdigest()

    # Redirect the mock scratch output so the test doesn't litter the repo.
    monkeypatch.setattr(nac, "MOCK_MAPPINGS_PATH", tmp_path / "mock.json")
    monkeypatch.setenv("LLM_MODE", "mock")

    session = get_session()
    session.add(Plan(id="p1", name="P", abbreviation="P", state="CA"))
    doc = Document(plan_id="p1", url="https://x/c.pdf", filename="c.pdf",
                   doc_type="cafr", extraction_status="done", fiscal_year=2025)
    session.add(doc)
    session.commit()
    ext = CafrExtract(plan_id="p1", document_id=doc.id, fiscal_year=2025)
    session.add(ext)
    session.commit()
    # A label overwhelmingly unlikely to already be in the committed mapping,
    # so main() has real work to do and reaches the _save call.
    session.add(CafrAllocation(cafr_extract_id=ext.id,
                               asset_class="Zzz Synthetic Test Sleeve"))
    session.commit()
    session.close()

    assert nac.main([]) == 0

    assert committed.read_bytes() == before
    assert hashlib.sha256(committed.read_bytes()).hexdigest() == digest_before

    # The mock run still did its work, just somewhere harmless.
    written = json.loads((tmp_path / "mock.json").read_text(encoding="utf-8"))
    assert written["Zzz Synthetic Test Sleeve"] == {"canonical": "unmapped",
                                                   "confidence": "low"}


def test_save_path_redirects_only_for_committed_file(monkeypatch, tmp_path):
    """The guard keys off the real config path, not the module attribute."""
    monkeypatch.setenv("LLM_MODE", "mock")
    assert nac._save_path() == nac.MOCK_MAPPINGS_PATH

    # Tests that point MAPPINGS_PATH at a scratch file still write there.
    scratch = tmp_path / "m.json"
    monkeypatch.setattr(nac, "MAPPINGS_PATH", scratch)
    assert nac._save_path() == scratch

    # Live mode always writes the real file.
    monkeypatch.setenv("LLM_MODE", "live")
    monkeypatch.setattr(nac, "MAPPINGS_PATH", nac._COMMITTED_MAPPINGS_PATH)
    assert nac._save_path() == nac._COMMITTED_MAPPINGS_PATH
