"""FastAPI app entry point. Run via: `uvicorn api.main:app --host 0.0.0.0 --port $PORT`."""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes.rfps import router as rfps_router
from api.routes import twins
from rfp.logging_setup import configure_logging

configure_logging()


async def _refresh_db_loop():
    """Re-pull the DB every 5 minutes while the service runs."""
    import database
    from scripts import db_sync
    while True:
        await asyncio.sleep(300)
        try:
            await asyncio.to_thread(
                db_sync.pull, database.DB_PATH,
                pre_replace=database.engine.dispose)
        except Exception as exc:  # noqa: BLE001
            import logging
            logging.getLogger("db_sync").warning("refresh failed: %s", exc)


@asynccontextmanager
async def lifespan(app):
    from scripts import db_sync
    task = None
    if db_sync.enabled():
        import database
        await asyncio.to_thread(
            db_sync.pull, database.DB_PATH,
            pre_replace=database.engine.dispose)
        task = asyncio.create_task(_refresh_db_loop())
    yield
    if task:
        task.cancel()


app = FastAPI(
    title="PensionPlanIntelligence RFP API",
    version="1.0.0",
    description="Read API for RFP records extracted from pension plan documents.",
    lifespan=lifespan,
)

_cors_origins = [o.strip() for o in os.environ.get("CORS_ORIGIN", "*").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins or ["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(rfps_router)
app.include_router(twins.router)
