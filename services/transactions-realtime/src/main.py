"""transactions-realtime: subscribes to new transactions for the configured
contract via an Infura websocket and produces them to Kafka.

This is a scaffold stub. Real Infura subscription and Kafka production is
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
logger = logging.getLogger("transactions-realtime")

_shutdown = False


def _handle_shutdown(signum: int, _frame: FrameType | None) -> None:
    global _shutdown
    logger.info("received signal %s, shutting down", signum)
    _shutdown = True


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    contract_address = os.environ.get("ETHERSCAN_CONTRACT_ADDRESS", "")
    poll_interval = os.environ.get("INFURA_POLL_INTERVAL", "")
    project_id_set = bool(os.environ.get("INFURA_PROJECT_ID"))
    kafka_broker = os.environ.get("KAFKA_BROKER_URL", "")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "")

    logger.info(
        "config: contract=%s poll_interval=%s infura_project_id_set=%s "
        "kafka_broker=%s kafka_topic=%s",
        contract_address,
        poll_interval,
        project_id_set,
        kafka_broker,
        kafka_topic,
    )
    logger.info("stub: Infura websocket subscription + Kafka produce not implemented yet")
    logger.info("started, waiting")

    while not _shutdown:
        time.sleep(5)


if __name__ == "__main__":
    main()
