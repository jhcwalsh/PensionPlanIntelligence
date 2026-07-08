"""The freshness helper re-pulls and disposes the engine on new generation."""
import pytest

pytest.importorskip("streamlit")  # CI installs requirements-pipeline.txt only


def test_ensure_fresh_db_disposes_engine_on_change(monkeypatch):
    """On a new generation, both the engine and the cached session must
    be cleared BEFORE the file swap (pre_replace), so the next request
    doesn't get served by a stale cached Session pointed at the old
    file's connections."""
    import app as app_module
    import database
    calls = []
    monkeypatch.setattr("scripts.db_sync.enabled", lambda: True)
    monkeypatch.setattr("scripts.db_sync.auto_push_pending", lambda: False)

    def fake_pull(dest, pre_replace=None):
        if pre_replace is not None:
            pre_replace()
        return True

    monkeypatch.setattr("scripts.db_sync.pull", fake_pull)
    monkeypatch.setattr(database.engine, "dispose",
                        lambda: calls.append("disposed"))
    monkeypatch.setattr(app_module.get_db_session, "clear",
                        lambda: calls.append("cleared"))
    app_module._ensure_fresh_db.clear()      # reset st.cache_data
    assert app_module._ensure_fresh_db() is True
    assert calls == ["disposed", "cleared"]


def test_ensure_fresh_db_noop_when_current(monkeypatch):
    import app as app_module
    import database
    calls = []
    monkeypatch.setattr("scripts.db_sync.enabled", lambda: True)
    monkeypatch.setattr("scripts.db_sync.auto_push_pending", lambda: False)
    monkeypatch.setattr("scripts.db_sync.pull",
                        lambda dest, pre_replace=None: False)
    monkeypatch.setattr(database.engine, "dispose",
                        lambda: calls.append("disposed"))
    monkeypatch.setattr(app_module.get_db_session, "clear",
                        lambda: calls.append("cleared"))
    app_module._ensure_fresh_db.clear()
    assert app_module._ensure_fresh_db() is False
    assert calls == []


def test_ensure_fresh_db_skips_pull_when_auto_push_pending(monkeypatch):
    """Never yank the local file out from under a pending local write."""
    import app as app_module
    calls = []
    monkeypatch.setattr("scripts.db_sync.enabled", lambda: True)
    monkeypatch.setattr("scripts.db_sync.auto_push_pending", lambda: True)

    def fake_pull(dest, pre_replace=None):
        calls.append("pulled")
        return True

    monkeypatch.setattr("scripts.db_sync.pull", fake_pull)
    app_module._ensure_fresh_db.clear()
    assert app_module._ensure_fresh_db() is False
    assert calls == []


def test_ensure_fresh_db_swallows_pull_errors(monkeypatch):
    """An R2 blip must never error-page the site."""
    import app as app_module
    monkeypatch.setattr("scripts.db_sync.enabled", lambda: True)
    monkeypatch.setattr("scripts.db_sync.auto_push_pending", lambda: False)

    def boom(dest, pre_replace=None):
        raise RuntimeError("network blip")

    monkeypatch.setattr("scripts.db_sync.pull", boom)
    app_module._ensure_fresh_db.clear()
    assert app_module._ensure_fresh_db() is False
