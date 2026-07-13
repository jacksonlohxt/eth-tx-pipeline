"""endpoint-server: FastAPI REST API over the processed transaction data
stored in MongoDB.

This scaffold only wires up `/health` and the auto-generated `/docs` page.
Real read endpoints over the `transactions` collection are round-2 work;
see docs/architecture.md and docs/message-schema.md for the document
shape they will expose.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("endpoint-server")


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    logger.info("started, waiting")
    yield


app = FastAPI(
    title="eth-tx-pipeline endpoint-server",
    description="REST API over enriched Ethereum transaction data.",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
