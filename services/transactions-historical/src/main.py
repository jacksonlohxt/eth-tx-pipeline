"""transactions-historical: fetches historical transactions for the configured
contract from the Etherscan API in batches and produces them to Kafka.

This is a scaffold stub. Real Etherscan pagination and Kafka production is
round-2 work; see docs/architecture.md for the component contract.
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
logger = logging.getLogger("transactions-historical")

_shutdown = False


def _handle_shutdown(signum: int, _frame: FrameType | None) -> None:
    global _shutdown
    logger.info("received signal %s, shutting down", signum)
    _shutdown = True


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    contract_address = os.environ.get("ETHERSCAN_CONTRACT_ADDRESS", "")
    first_block = os.environ.get("ETHERSCAN_HISTORICAL_FIRST_BLOCK", "")
    last_block = os.environ.get("ETHERSCAN_HISTORICAL_LAST_BLOCK", "")
    batch_size = os.environ.get("ETHERSCAN_HISTORICAL_BATCH_SIZE", "")
    kafka_broker = os.environ.get("KAFKA_BROKER_URL", "")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "")

    logger.info(
        "config: contract=%s blocks=[%s, %s] batch_size=%s kafka_broker=%s kafka_topic=%s",
        contract_address,
        first_block,
        last_block,
        batch_size,
        kafka_broker,
        kafka_topic,
    )
    logger.info("stub: Etherscan batch fetch + Kafka produce not implemented yet")
    logger.info("started, waiting")

    while not _shutdown:
        time.sleep(5)


if __name__ == "__main__":
    main()
