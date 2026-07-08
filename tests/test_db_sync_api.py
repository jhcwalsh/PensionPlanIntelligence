"""API lifespan pulls the DB when sync is enabled."""
import asyncio


def test_lifespan_pulls_when_enabled(monkeypatch):
    from api import main as api_main
    pulled = []
    monkeypatch.setattr("scripts.db_sync.enabled", lambda: True)
    monkeypatch.setattr("scripts.db_sync.pull",
                        lambda dest: pulled.append(str(dest)) or True)

    async def run():
        async with api_main.lifespan(api_main.app):
            pass
    asyncio.run(run())
    assert len(pulled) == 1
