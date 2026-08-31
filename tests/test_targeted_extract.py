"""Storing and extracting what a targeted read of a document finds."""
import database
from database import Document, DocumentSectionRead, Plan


def test_section_reads_sit_beside_the_document(tmp_db):
    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA")); s.commit()
    d = Document(plan_id="mcera", url="https://x/a.pdf", filename="a.pdf",
                 extracted_text="the whole pack")
    s.add(d); s.commit()
    before = d.extracted_text

    s.add(DocumentSectionRead(document_id=d.id, offset=200_881,
                              heading="Total Rates of Return (%)",
                              returns_json='[{"asset_class":"US Equity","return_pct":12.4}]'))
    s.commit(); s.expire_all()

    assert s.get(Document, d.id).extracted_text == before
    assert s.query(DocumentSectionRead).one().offset == 200_881
    s.close()


def test_the_same_passage_cannot_be_charged_for_twice(tmp_db):
    """UNIQUE(document_id, offset) is what makes a re-run a no-op.

    Without it, re-running the CLI over a document whose read failed to be
    recorded charges again for the identical window.
    """
    import pytest
    from sqlalchemy.exc import IntegrityError

    s = database.get_session()
    s.add(Plan(id="mcera", name="MCERA", state="CA")); s.commit()
    d = Document(plan_id="mcera", url="https://x/a.pdf", filename="a.pdf")
    s.add(d); s.commit()

    s.add(DocumentSectionRead(document_id=d.id, offset=1000, returns_json="[]"))
    s.commit()
    s.add(DocumentSectionRead(document_id=d.id, offset=1000, returns_json="[]"))
    with pytest.raises(IntegrityError):
        s.commit()
    s.rollback(); s.close()
