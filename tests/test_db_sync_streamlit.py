"""The freshness helper re-pulls and disposes the engine on new generation."""
import pytest

pytest.importorskip("streamlit")  # CI installs requirements-pipeline.txt only


def test_ensure_fresh_db_disposes_engine_on_change(monkeypatch):
    import app as app_module
    import database
    calls = []
    monkeypatch.setattr("scripts.db_sync.enabled", lambda: True)
    monkeypatch.setattr("scripts.db_sync.pull", lambda dest: True)
    monkeypatch.setattr(database.engine, "dispose",
                        lambda: calls.append("disposed"))
    app_module._ensure_fresh_db.clear()      # reset st.cache_data
    app_module._ensure_fresh_db()
    assert calls == ["disposed"]


def test_ensure_fresh_db_noop_when_current(monkeypatch):
    import app as app_module
    import database
    calls = []
    monkeypatch.setattr("scripts.db_sync.enabled", lambda: True)
    monkeypatch.setattr("scripts.db_sync.pull", lambda dest: False)
    monkeypatch.setattr(database.engine, "dispose",
                        lambda: calls.append("disposed"))
    app_module._ensure_fresh_db.clear()
    app_module._ensure_fresh_db()
    assert calls == []
