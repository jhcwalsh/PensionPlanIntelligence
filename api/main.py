"""FastAPI app entry point. Run via: `uvicorn api.main:app --host 0.0.0.0 --port $PORT`."""

from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes.rfps import router as rfps_router
from rfp.logging_setup import configure_logging

configure_logging()

app = FastAPI(
    title="PensionPlanIntelligence RFP API",
    version="1.0.0",
    description="Read API for RFP records extracted from pension plan documents.",
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
