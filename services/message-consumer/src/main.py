"""Consume Ethereum transactions from Kafka, enrich them, and upsert MongoDB."""

from __future__ import annotations

import logging
import math
import os
import signal
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import FrameType
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException
from eth_tx_shared.schema import EnrichedTransaction, TransactionMessage
from pymongo import MongoClient
from pymongo.collection import Collection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("message-consumer")

COLLECTION_NAME = "transactions"
DEFAULT_ETH_USD_EXCHANGE_RATE = 3_000.0
_shutdown = False


@dataclass(frozen=True, slots=True)
class Settings:
    kafka_broker: str
    kafka_topic: str
    kafka_group_id: str
    mongodb_url: str
    eth_usd_exchange_rate: float

    @classmethod
    def from_env(cls, environ: Mapping[str, str] = os.environ) -> Settings:
        rate = float(
            environ.get("ETH_USD_EXCHANGE_RATE", str(DEFAULT_ETH_USD_EXCHANGE_RATE))
        )
        if not math.isfinite(rate) or rate <= 0:
            raise ValueError("ETH_USD_EXCHANGE_RATE must be a positive finite number")

        return cls(
            kafka_broker=environ.get("KAFKA_BROKER_URL", "localhost:9092"),
            kafka_topic=environ.get("KAFKA_TOPIC", "transactions"),
            kafka_group_id=environ.get("KAFKA_GROUP_ID", "message-consumer"),
            mongodb_url=environ.get(
                "MONGODB_URL", "mongodb://localhost:27017/eth_tx_pipeline"
            ),
            eth_usd_exchange_rate=rate,
        )


def _handle_shutdown(signum: int, _frame: FrameType | None) -> None:
    global _shutdown
    logger.info("received signal %s, shutting down", signum)
    _shutdown = True


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def enrich_transaction(
    message: TransactionMessage,
    *,
    eth_usd_exchange_rate: float,
    enriched_at: str,
) -> EnrichedTransaction:
    fee_eth = message.gas_price_wei * message.gas_used / 1e18
    fee_usd = fee_eth * eth_usd_exchange_rate
    return EnrichedTransaction.from_message(
        message,
        fee_eth=fee_eth,
        fee_usd=fee_usd,
        eth_usd_exchange_rate=eth_usd_exchange_rate,
        enriched_at=enriched_at,
    )


def consume_messages(
    consumer: Consumer,
    collection: Collection[dict[str, Any]],
    *,
    eth_usd_exchange_rate: float,
    stop_requested: Callable[[], bool] = lambda: _shutdown,
    timestamp_factory: Callable[[], str] = utc_now_iso,
    max_messages: int | None = None,
) -> int:
    """Consume and persist messages, committing Kafka only after each Mongo upsert."""
    processed = 0
    while not stop_requested():
        kafka_message = consumer.poll(timeout=1.0)
        if kafka_message is None:
            continue

        error = kafka_message.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(error)

        raw_value = kafka_message.value()
        if raw_value is None:
            raise ValueError("transaction message has no value")

        transaction = TransactionMessage.from_json(raw_value)
        enriched = enrich_transaction(
            transaction,
            eth_usd_exchange_rate=eth_usd_exchange_rate,
            enriched_at=timestamp_factory(),
        )
        document = enriched.to_mongo_document()
        collection.replace_one({"_id": document["_id"]}, document, upsert=True)

        # Synchronous commit preserves at-least-once delivery. A crash between the
        # Mongo upsert and this commit safely replays into the same `_id`.
        consumer.commit(message=kafka_message, asynchronous=False)
        processed += 1
        logger.info("upserted transaction tx_hash=%s", transaction.tx_hash)

        if max_messages is not None and processed >= max_messages:
            break

    return processed


def _new_consumer(settings: Settings) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_broker,
            "group.id": settings.kafka_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def main() -> None:
    global _shutdown
    _shutdown = False
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    settings = Settings.from_env()
    logger.info(
        "config: kafka_broker=%s kafka_topic=%s kafka_group_id=%s "
        "mongo_database=%s eth_usd_exchange_rate=%s",
        settings.kafka_broker,
        settings.kafka_topic,
        settings.kafka_group_id,
        settings.mongodb_url.rsplit("/", maxsplit=1)[-1],
        settings.eth_usd_exchange_rate,
    )

    mongo_client: MongoClient[dict[str, Any]] = MongoClient(
        settings.mongodb_url, serverSelectionTimeoutMS=10_000
    )
    consumer = _new_consumer(settings)
    try:
        collection = mongo_client.get_default_database()[COLLECTION_NAME]
        consumer.subscribe([settings.kafka_topic])
        logger.info("started, waiting for transaction messages")
        consume_messages(
            consumer,
            collection,
            eth_usd_exchange_rate=settings.eth_usd_exchange_rate,
        )
    finally:
        consumer.close()
        mongo_client.close()
        logger.info("shutdown complete")


if __name__ == "__main__":
    main()
