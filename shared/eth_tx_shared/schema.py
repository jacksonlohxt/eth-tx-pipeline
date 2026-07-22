"""Shared wire/document schema for the eth-tx-pipeline services.

Two shapes are defined here:

- ``TransactionMessage``: the JSON payload the producer services
  (transactions-historical, transactions-realtime) publish to the Kafka
  topic. It carries a transaction exactly as observed on-chain, in wei/gwei
  base units, plus provenance about how it was ingested.
- ``EnrichedTransaction``: the MongoDB document message-consumer writes
  after enrichment. It embeds the original message fields and adds the
  computed fee/exchange-rate fields.

See docs/message-schema.md for the authoritative field-by-field
description, worked examples, and the rationale for keeping this schema
as a single shared package rather than duplicating dataclasses per
service.
"""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class TransactionMessage:
    """A single Ethereum transaction as published to the Kafka topic."""

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

    def to_json(self) -> str:
        return json.dumps(dataclasses.asdict(self))

    @classmethod
    def from_json(cls, raw: str | bytes) -> TransactionMessage:
        return cls(**json.loads(raw))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TransactionMessage:
        return cls(**data)


@dataclass(frozen=True, slots=True)
class EnrichedTransaction:
    """The MongoDB document produced by message-consumer.

    Embeds every ``TransactionMessage`` field plus the enrichment results.
    """

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

    @classmethod
    def from_message(
        cls,
        message: TransactionMessage,
        *,
        fee_eth: float,
        fee_usd: float,
        eth_usd_exchange_rate: float,
        enriched_at: str,
    ) -> EnrichedTransaction:
        return cls(
            **dataclasses.asdict(message),
            fee_eth=fee_eth,
            fee_usd=fee_usd,
            eth_usd_exchange_rate=eth_usd_exchange_rate,
            enriched_at=enriched_at,
        )

    def to_mongo_document(self) -> dict[str, Any]:
        """Mongo document keyed by ``_id=tx_hash`` for natural idempotency."""
        doc = dataclasses.asdict(self)
        doc["_id"] = doc["tx_hash"]
        return doc
