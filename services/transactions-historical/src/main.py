"""Fetch historical Ethereum transactions from Etherscan and publish to Kafka."""

from __future__ import annotations

import json
import logging
import os
import signal
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import Any, Protocol

from confluent_kafka import Producer
from eth_tx_shared.schema import TransactionMessage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("transactions-historical")

DEFAULT_CONTRACT_ADDRESS = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
DEFAULT_ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"
DEFAULT_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "etherscan_txlist.json"
)
DEFAULT_PAGE_SIZE = 10_000
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_KAFKA_FLUSH_TIMEOUT_SECONDS = 30.0
_shutdown = False


class KafkaProducer(Protocol):
    """The subset of confluent-kafka Producer used by the backfill."""

    def produce(
        self,
        topic: str,
        *,
        key: str,
        value: str,
        on_delivery: Callable[[Any, Any], None],
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


@dataclass(frozen=True, slots=True)
class Settings:
    contract_address: str
    first_block: int
    last_block: int
    batch_size: int
    etherscan_api_key: str
    kafka_broker: str
    kafka_topic: str
    fixture_path: Path = DEFAULT_FIXTURE_PATH
    etherscan_api_url: str = DEFAULT_ETHERSCAN_API_URL
    etherscan_chain_id: int = 1
    etherscan_page_size: int = DEFAULT_PAGE_SIZE

    @classmethod
    def from_env(cls, environ: Mapping[str, str] = os.environ) -> Settings:
        settings = cls(
            contract_address=environ.get(
                "ETHERSCAN_CONTRACT_ADDRESS", DEFAULT_CONTRACT_ADDRESS
            ).strip(),
            first_block=int(environ.get("ETHERSCAN_HISTORICAL_FIRST_BLOCK", "18000000")),
            last_block=int(environ.get("ETHERSCAN_HISTORICAL_LAST_BLOCK", "18001000")),
            batch_size=int(environ.get("ETHERSCAN_HISTORICAL_BATCH_SIZE", "100")),
            etherscan_api_key=environ.get("ETHERSCAN_API_KEY", "").strip(),
            kafka_broker=environ.get("KAFKA_BROKER_URL", "localhost:9092"),
            kafka_topic=environ.get("KAFKA_TOPIC", "transactions"),
            fixture_path=Path(environ.get("ETHERSCAN_FIXTURE_PATH", str(DEFAULT_FIXTURE_PATH))),
            etherscan_api_url=environ.get("ETHERSCAN_API_URL", DEFAULT_ETHERSCAN_API_URL),
            etherscan_chain_id=int(environ.get("ETHERSCAN_CHAIN_ID", "1")),
            etherscan_page_size=int(
                environ.get("ETHERSCAN_HISTORICAL_PAGE_SIZE", str(DEFAULT_PAGE_SIZE))
            ),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if not self.contract_address:
            raise ValueError("ETHERSCAN_CONTRACT_ADDRESS must not be empty")
        if self.first_block < 0 or self.last_block < self.first_block:
            raise ValueError("historical block range must be non-negative and ordered")
        if self.batch_size <= 0:
            raise ValueError("ETHERSCAN_HISTORICAL_BATCH_SIZE must be positive")
        if self.etherscan_chain_id <= 0:
            raise ValueError("ETHERSCAN_CHAIN_ID must be positive")
        if self.etherscan_page_size <= 0:
            raise ValueError("ETHERSCAN_HISTORICAL_PAGE_SIZE must be positive")
        if not self.kafka_broker or not self.kafka_topic:
            raise ValueError("Kafka broker and topic must not be empty")


def _handle_shutdown(signum: int, _frame: FrameType | None) -> None:
    global _shutdown
    logger.info("received signal %s, shutting down", signum)
    _shutdown = True


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _transactions_from_response(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise ValueError("Etherscan response must be a JSON object")

    status = str(payload.get("status", ""))
    result = payload.get("result")
    if status == "1" and isinstance(result, list):
        transactions = result
    elif status == "0" and result == []:
        transactions = []
    elif status == "0" and str(payload.get("message", "")).lower().startswith("no transactions"):
        transactions = []
    else:
        message = payload.get("message", "unknown Etherscan error")
        raise RuntimeError(f"Etherscan API rejected txlist request: {message}")

    if not all(isinstance(transaction, dict) for transaction in transactions):
        raise ValueError("Etherscan txlist result must contain JSON objects")
    return transactions


def load_fixture(path: Path) -> list[dict[str, Any]]:
    """Load and validate a recorded Etherscan txlist response."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"could not load Etherscan fixture at {path}") from exc
    return _transactions_from_response(payload)


def _request_etherscan_page(
    settings: Settings,
    *,
    start_block: int,
    end_block: int,
    page: int,
    urlopen: Callable[..., Any],
) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode(
        {
            "chainid": settings.etherscan_chain_id,
            "module": "account",
            "action": "txlist",
            "address": settings.contract_address,
            "startblock": start_block,
            "endblock": end_block,
            "page": page,
            "offset": settings.etherscan_page_size,
            "sort": "asc",
            "apikey": settings.etherscan_api_key,
        }
    )
    url = f"{settings.etherscan_api_url}?{query}"
    try:
        with urlopen(url, timeout=DEFAULT_REQUEST_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read())
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
        # Do not include the URL because its query string contains the API key.
        raise RuntimeError(
            f"Etherscan request failed for blocks [{start_block}, {end_block}] page {page}"
        ) from exc
    return _transactions_from_response(payload)


def fetch_etherscan_transactions(
    settings: Settings,
    *,
    urlopen: Callable[..., Any] = urllib.request.urlopen,
) -> list[dict[str, Any]]:
    """Fetch the configured inclusive block range in block and API-page batches."""
    transactions: list[dict[str, Any]] = []
    start_block = settings.first_block
    while start_block <= settings.last_block and not _shutdown:
        end_block = min(
            start_block + settings.batch_size - 1,
            settings.last_block,
        )
        page = 1
        while not _shutdown:
            page_transactions = _request_etherscan_page(
                settings,
                start_block=start_block,
                end_block=end_block,
                page=page,
                urlopen=urlopen,
            )
            transactions.extend(page_transactions)
            logger.info(
                "fetched %s transactions for blocks=[%s, %s] page=%s",
                len(page_transactions),
                start_block,
                end_block,
                page,
            )
            if len(page_transactions) < settings.etherscan_page_size:
                break
            page += 1
        start_block = end_block + 1
    return transactions


def transaction_from_etherscan(
    raw: Mapping[str, Any],
    *,
    contract_address: str,
    ingested_at: str,
) -> TransactionMessage:
    """Convert Etherscan's string-heavy txlist shape to the shared wire schema."""
    try:
        return TransactionMessage(
            tx_hash=str(raw["hash"]),
            block_number=int(raw["blockNumber"]),
            block_timestamp=int(raw["timeStamp"]),
            from_address=str(raw["from"]),
            to_address=str(raw["to"]),
            value_wei=int(raw["value"]),
            gas_price_wei=int(raw["gasPrice"]),
            gas_used=int(raw["gasUsed"]),
            contract_address=contract_address,
            source="historical",
            ingested_at=ingested_at,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("invalid transaction in Etherscan txlist response") from exc


def publish_messages(
    producer: KafkaProducer,
    *,
    topic: str,
    messages: Sequence[TransactionMessage],
) -> int:
    """Publish messages and wait until Kafka acknowledges every delivery."""
    delivery_errors: list[str] = []

    def on_delivery(error: Any, _message: Any) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    for message in messages:
        while True:
            try:
                producer.produce(
                    topic,
                    key=message.tx_hash,
                    value=message.to_json(),
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                producer.poll(1.0)
        producer.poll(0)

    outstanding = producer.flush(DEFAULT_KAFKA_FLUSH_TIMEOUT_SECONDS)
    if outstanding:
        raise RuntimeError(f"Kafka delivery timed out with {outstanding} messages pending")
    if delivery_errors:
        raise RuntimeError(f"Kafka delivery failed: {delivery_errors[0]}")
    return len(messages)


def run_historical_backfill(
    settings: Settings,
    producer: KafkaProducer,
    *,
    urlopen: Callable[..., Any] = urllib.request.urlopen,
    timestamp_factory: Callable[[], str] = utc_now_iso,
) -> int:
    """Run either credential-backed Etherscan mode or credential-free fixture mode."""
    if settings.etherscan_api_key:
        logger.info("ETHERSCAN_API_KEY is set; using live Etherscan API mode")
        raw_transactions = fetch_etherscan_transactions(settings, urlopen=urlopen)
    else:
        logger.info(
            "ETHERSCAN_API_KEY is unset; using credential-free recorded fixture mode (%s)",
            settings.fixture_path,
        )
        fixture_transactions = load_fixture(settings.fixture_path)
        try:
            raw_transactions = [
                transaction
                for transaction in fixture_transactions
                if settings.first_block <= int(transaction["blockNumber"]) <= settings.last_block
            ]
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid transaction in Etherscan fixture") from exc

    messages = [
        transaction_from_etherscan(
            transaction,
            contract_address=settings.contract_address,
            ingested_at=timestamp_factory(),
        )
        for transaction in raw_transactions
    ]
    published = publish_messages(producer, topic=settings.kafka_topic, messages=messages)
    logger.info("published %s historical transactions to %s", published, settings.kafka_topic)
    return published


def _new_producer(settings: Settings) -> Producer:
    return Producer({"bootstrap.servers": settings.kafka_broker})


def main() -> None:
    global _shutdown
    _shutdown = False
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    settings = Settings.from_env()
    logger.info(
        "config: contract=%s blocks=[%s, %s] batch_size=%s kafka_broker=%s kafka_topic=%s",
        settings.contract_address,
        settings.first_block,
        settings.last_block,
        settings.batch_size,
        settings.kafka_broker,
        settings.kafka_topic,
    )
    run_historical_backfill(settings, _new_producer(settings))


if __name__ == "__main__":
    main()
