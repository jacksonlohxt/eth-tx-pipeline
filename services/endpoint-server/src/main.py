"""FastAPI read API over enriched Ethereum transactions in MongoDB."""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("endpoint-server")

COLLECTION_NAME = "transactions"
DEFAULT_LIMIT = 50
MAX_LIMIT = 100


@dataclass(frozen=True, slots=True)
class Settings:
    mongodb_url: str

    @classmethod
    def from_env(cls, environ: Mapping[str, str] = os.environ) -> Settings:
        return cls(
            mongodb_url=environ.get(
                "MONGODB_URL", "mongodb://localhost:27017/eth_tx_pipeline"
            )
        )


class EnrichedTransactionResponse(BaseModel):
    """Public representation of an EnrichedTransaction MongoDB document."""

    model_config = ConfigDict(populate_by_name=True)

    mongo_id: str = Field(alias="_id", description="MongoDB identifier, equal to tx_hash")
    tx_hash: str
    block_number: int
    block_timestamp: int
    from_address: str
    to_address: str
    value_wei: int
    gas_price_wei: int
    gas_used: int
    contract_address: str
    source: str
    ingested_at: str
    fee_eth: float
    fee_usd: float
    eth_usd_exchange_rate: float
    enriched_at: str


class TransactionPage(BaseModel):
    items: list[EnrichedTransactionResponse]
    total: int = Field(description="Total documents matching the filters")
    offset: int
    limit: int


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = Settings.from_env()
    mongo_client: MongoClient[dict[str, Any]] = MongoClient(
        settings.mongodb_url, serverSelectionTimeoutMS=10_000
    )
    app.state.mongo_client = mongo_client
    app.state.transactions_collection = mongo_client.get_default_database()[COLLECTION_NAME]
    logger.info(
        "started, reading mongo database=%s",
        settings.mongodb_url.rsplit("/", maxsplit=1)[-1],
    )
    try:
        yield
    finally:
        mongo_client.close()
        logger.info("shutdown complete")


app = FastAPI(
    title="eth-tx-pipeline endpoint-server",
    description="REST API over enriched Ethereum transaction data.",
    version="0.2.0",
    lifespan=lifespan,
)


def get_transactions_collection(request: Request) -> Collection[dict[str, Any]]:
    return request.app.state.transactions_collection


def _validate_range(
    lower: int | None,
    upper: int | None,
    *,
    lower_name: str,
    upper_name: str,
) -> None:
    if lower is not None and upper is not None and lower > upper:
        raise HTTPException(
            status_code=422,
            detail=f"{lower_name} must be less than or equal to {upper_name}",
        )


def _transaction_query(
    *,
    address: str | None,
    block_number_from: int | None,
    block_number_to: int | None,
    timestamp_from: int | None,
    timestamp_to: int | None,
) -> dict[str, Any]:
    query: dict[str, Any] = {}
    if address is not None:
        query["$or"] = [{"from_address": address}, {"to_address": address}]

    block_range: dict[str, int] = {}
    if block_number_from is not None:
        block_range["$gte"] = block_number_from
    if block_number_to is not None:
        block_range["$lte"] = block_number_to
    if block_range:
        query["block_number"] = block_range

    timestamp_range: dict[str, int] = {}
    if timestamp_from is not None:
        timestamp_range["$gte"] = timestamp_from
    if timestamp_to is not None:
        timestamp_range["$lte"] = timestamp_to
    if timestamp_range:
        query["block_timestamp"] = timestamp_range

    return query


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get(
    "/transactions",
    response_model=TransactionPage,
    summary="List enriched transactions",
)
def list_transactions(
    collection: Annotated[
        Collection[dict[str, Any]], Depends(get_transactions_collection)
    ],
    address: Annotated[
        str | None,
        Query(
            min_length=1,
            max_length=42,
            description="Exact sender or recipient Ethereum address",
        ),
    ] = None,
    block_number_from: Annotated[
        int | None,
        Query(ge=0, description="Inclusive minimum block number"),
    ] = None,
    block_number_to: Annotated[
        int | None,
        Query(ge=0, description="Inclusive maximum block number"),
    ] = None,
    timestamp_from: Annotated[
        int | None,
        Query(ge=0, description="Inclusive minimum block timestamp in Unix seconds"),
    ] = None,
    timestamp_to: Annotated[
        int | None,
        Query(ge=0, description="Inclusive maximum block timestamp in Unix seconds"),
    ] = None,
    offset: Annotated[int, Query(ge=0, description="Number of matching documents to skip")] = 0,
    limit: Annotated[
        int,
        Query(ge=1, le=MAX_LIMIT, description="Maximum documents to return"),
    ] = DEFAULT_LIMIT,
) -> TransactionPage:
    """Return a deterministic offset/limit page ordered by block number and tx hash."""
    _validate_range(
        block_number_from,
        block_number_to,
        lower_name="block_number_from",
        upper_name="block_number_to",
    )
    _validate_range(
        timestamp_from,
        timestamp_to,
        lower_name="timestamp_from",
        upper_name="timestamp_to",
    )
    query = _transaction_query(
        address=address,
        block_number_from=block_number_from,
        block_number_to=block_number_to,
        timestamp_from=timestamp_from,
        timestamp_to=timestamp_to,
    )

    documents = list(
        collection.find(query)
        .sort([("block_number", ASCENDING), ("_id", ASCENDING)])
        .skip(offset)
        .limit(limit)
    )
    return TransactionPage(
        items=[EnrichedTransactionResponse.model_validate(document) for document in documents],
        total=collection.count_documents(query),
        offset=offset,
        limit=limit,
    )
