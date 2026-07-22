"""db-indexing-sidecar: ensures the MongoDB indexes the rest of the
pipeline depends on exist, then exits.

Unlike the other services in this scaffold, this one's entire job is the
"stub" behavior: connect, create indexes, exit 0. There is no round-2
follow-up work for this service beyond adding indexes as query patterns
in endpoint-server evolve.
"""

from __future__ import annotations

import logging
import os

from pymongo import ASCENDING, MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("db-indexing-sidecar")

COLLECTION_NAME = "transactions"


def index_specs() -> list[tuple[str, int]]:
    """Fields indexed on the `transactions` collection.

    `_id` is `tx_hash` (see docs/message-schema.md) and is already
    uniquely indexed by MongoDB, so it is not repeated here.
    """
    return [
        ("block_number", ASCENDING),
        ("contract_address", ASCENDING),
        ("block_timestamp", ASCENDING),
    ]


def main() -> None:
    mongodb_url = os.environ.get("MONGODB_URL", "mongodb://localhost:27017")
    logger.info("config: mongodb_url=%s", mongodb_url)

    client = MongoClient(mongodb_url, serverSelectionTimeoutMS=10_000)
    try:
        collection = client.get_default_database()[COLLECTION_NAME]
        for field, direction in index_specs():
            collection.create_index([(field, direction)])
            logger.info("ensured index on %s", field)
    finally:
        client.close()

    logger.info("indexing complete, exiting")


if __name__ == "__main__":
    main()
