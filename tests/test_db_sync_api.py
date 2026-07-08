"""API lifespan pulls the DB when sync is enabled."""
import asyncio


def test_lifespan_pulls_when_enabled(monkeypatch):
    from api import main as api_main
    pulled = []
    monkeypatch.setattr("scripts.db_sync.enabled", lambda: True)
    monkeypatch.setattr(
        "scripts.db_sync.pull",
        lambda dest, pre_replace=None: pulled.append(str(dest)) or True)

    async def run():
        async with api_main.lifespan(api_main.app):
            pass
    asyncio.run(run())
    assert len(pulled) == 1


def test_lifespan_passes_pre_replace_that_disposes_engine(monkeypatch):
    """pre_replace=database.engine.dispose, not a post-pull dispose call.

    Confirms the engine is only disposed via the pre_replace callback
    (exactly once per pull) rather than a separate after-the-fact
    dispose that used to run regardless of whether pull's own
    pre_replace already handled it.
    """
    from api import main as api_main
    import database
    calls = []
    monkeypatch.setattr("scripts.db_sync.enabled", lambda: True)
    monkeypatch.setattr(database.engine, "dispose",
                        lambda: calls.append("disposed"))

    def fake_pull(dest, pre_replace=None):
        if pre_replace is not None:
            pre_replace()
        return True

    monkeypatch.setattr("scripts.db_sync.pull", fake_pull)

    async def run():
        async with api_main.lifespan(api_main.app):
            pass
    asyncio.run(run())
    assert calls == ["disposed"]
