"""message-consumer: consumes transaction messages from Kafka, enriches
each with fee-in-ETH / fee-in-USD using an ETH/USD exchange rate, and
writes the resulting document to MongoDB.

This is a scaffold stub. Real Kafka consumption, enrichment, and Mongo
writes are round-2 work; see docs/architecture.md and
docs/message-schema.md for the component contract.
"""

from __future__ import annotations

import logging
import os
import signal
import time
from types import FrameType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("message-consumer")

_shutdown = False


def _handle_shutdown(signum: int, _frame: FrameType | None) -> None:
    global _shutdown
    logger.info("received signal %s, shutting down", signum)
    _shutdown = True


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    kafka_broker = os.environ.get("KAFKA_BROKER_URL", "")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "")
    kafka_group_id = os.environ.get("KAFKA_GROUP_ID", "")
    mongodb_url = os.environ.get("MONGODB_URL", "")

    logger.info(
        "config: kafka_broker=%s kafka_topic=%s kafka_group_id=%s mongodb_url=%s",
        kafka_broker,
        kafka_topic,
        kafka_group_id,
        mongodb_url,
    )
    logger.info("stub: Kafka consume + enrichment + Mongo write not implemented yet")
    logger.info("started, waiting")

    while not _shutdown:
        time.sleep(5)


if __name__ == "__main__":
    main()
