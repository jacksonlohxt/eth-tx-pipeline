"""Stream contract activity from Infura to Kafka with a polling fallback."""

from __future__ import annotations

import json
import logging
import os
import signal
import time
import urllib.error
import urllib.request
from collections import deque
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from types import FrameType
from typing import Any, Protocol

from confluent_kafka import Producer
from eth_tx_shared.schema import TransactionMessage
from websockets.sync.client import connect as websocket_connect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("transactions-realtime")

DEFAULT_CONTRACT_ADDRESS = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
DEFAULT_POLL_INTERVAL_SECONDS = 15.0
DEFAULT_WEBSOCKET_RETRY_SECONDS = 30.0
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30.0
DEFAULT_POLL_BLOCK_RANGE = 1_000
DEFAULT_SEEN_TRANSACTION_LIMIT = 10_000
DEFAULT_KAFKA_FLUSH_TIMEOUT_SECONDS = 30.0
WEBSOCKET_RECEIVE_TIMEOUT_SECONDS = 1.0
_shutdown = False


class KafkaProducer(Protocol):
    """The subset of confluent-kafka Producer used by this service."""

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


class RpcClient(Protocol):
    def call(self, method: str, params: list[Any] | None = None) -> Any: ...


class WebSocket(Protocol):
    def send(self, message: str) -> None: ...

    def recv(self, timeout: float | None = None) -> str | bytes: ...


WebSocketFactory = Callable[..., AbstractContextManager[WebSocket]]


@dataclass(frozen=True, slots=True)
class Settings:
    contract_address: str
    infura_project_id: str
    kafka_broker: str
    kafka_topic: str
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS
    websocket_retry_interval: float = DEFAULT_WEBSOCKET_RETRY_SECONDS
    request_timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS
    poll_block_range: int = DEFAULT_POLL_BLOCK_RANGE

    @classmethod
    def from_env(cls, environ: Mapping[str, str] = os.environ) -> Settings:
        settings = cls(
            contract_address=environ.get(
                "ETHERSCAN_CONTRACT_ADDRESS", DEFAULT_CONTRACT_ADDRESS
            ).strip(),
            infura_project_id=environ.get("INFURA_PROJECT_ID", "").strip(),
            kafka_broker=environ.get("KAFKA_BROKER_URL", "localhost:9092").strip(),
            kafka_topic=environ.get("KAFKA_TOPIC", "transactions").strip(),
            poll_interval=float(
                environ.get("INFURA_POLL_INTERVAL", str(DEFAULT_POLL_INTERVAL_SECONDS))
            ),
            websocket_retry_interval=float(
                environ.get(
                    "INFURA_WEBSOCKET_RETRY_INTERVAL",
                    str(DEFAULT_WEBSOCKET_RETRY_SECONDS),
                )
            ),
            request_timeout=float(
                environ.get("INFURA_REQUEST_TIMEOUT", str(DEFAULT_REQUEST_TIMEOUT_SECONDS))
            ),
            poll_block_range=int(
                environ.get("INFURA_POLL_BLOCK_RANGE", str(DEFAULT_POLL_BLOCK_RANGE))
            ),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if not self.contract_address:
            raise ValueError("ETHERSCAN_CONTRACT_ADDRESS must not be empty")
        if not self.infura_project_id:
            raise ValueError("INFURA_PROJECT_ID must be set for realtime ingestion")
        if not self.kafka_broker or not self.kafka_topic:
            raise ValueError("Kafka broker and topic must not be empty")
        if self.poll_interval <= 0:
            raise ValueError("INFURA_POLL_INTERVAL must be positive")
        if self.websocket_retry_interval < 0:
            raise ValueError("INFURA_WEBSOCKET_RETRY_INTERVAL must not be negative")
        if self.request_timeout <= 0:
            raise ValueError("INFURA_REQUEST_TIMEOUT must be positive")
        if self.poll_block_range <= 0:
            raise ValueError("INFURA_POLL_BLOCK_RANGE must be positive")

    @property
    def http_url(self) -> str:
        return f"https://mainnet.infura.io/v3/{self.infura_project_id}"

    @property
    def websocket_url(self) -> str:
        return f"wss://mainnet.infura.io/ws/v3/{self.infura_project_id}"


class InfuraRpcClient:
    """Small JSON-RPC client for the HTTP calls needed to hydrate log events."""

    def __init__(self, url: str, *, timeout: float) -> None:
        self._url = url
        self._timeout = timeout
        self._request_id = 0

    def call(self, method: str, params: list[Any] | None = None) -> Any:
        self._request_id += 1
        body = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": self._request_id,
                "method": method,
                "params": params or [],
            }
        ).encode()
        request = urllib.request.Request(
            self._url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                payload = json.loads(response.read())
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Infura JSON-RPC request failed for {method}") from exc

        if not isinstance(payload, dict):
            raise RuntimeError(f"Infura returned an invalid JSON-RPC response for {method}")
        if payload.get("error") is not None:
            error = payload["error"]
            if isinstance(error, dict):
                detail = error.get("message", "unknown error")
            else:
                detail = "unknown error"
            raise RuntimeError(f"Infura JSON-RPC error for {method}: {detail}")
        if "result" not in payload:
            raise RuntimeError(f"Infura JSON-RPC response for {method} has no result")
        return payload["result"]


class SeenTransactions:
    """Bounded duplicate filter for transactions that emit multiple contract logs."""

    def __init__(self, limit: int = DEFAULT_SEEN_TRANSACTION_LIMIT) -> None:
        self._limit = limit
        self._queue: deque[str] = deque()
        self._values: set[str] = set()

    def __contains__(self, tx_hash: str) -> bool:
        return tx_hash in self._values

    def add(self, tx_hash: str) -> None:
        if tx_hash in self._values:
            return
        self._queue.append(tx_hash)
        self._values.add(tx_hash)
        if len(self._queue) > self._limit:
            self._values.remove(self._queue.popleft())


def _handle_shutdown(signum: int, _frame: FrameType | None) -> None:
    global _shutdown
    logger.info("received signal %s, shutting down", signum)
    _shutdown = True


def _is_shutdown() -> bool:
    return _shutdown


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _hex_int(value: Any, *, field: str) -> int:
    if isinstance(value, int):
        return value
    if not isinstance(value, str):
        raise ValueError(f"Infura field {field} must be a hexadecimal string")
    try:
        return int(value, 16)
    except ValueError as exc:
        raise ValueError(f"Infura field {field} is not valid hexadecimal") from exc


def transaction_from_log(
    log: Mapping[str, Any],
    *,
    contract_address: str,
    rpc_client: RpcClient,
    ingested_at: str,
) -> TransactionMessage:
    """Hydrate one contract log into the shared transaction wire schema."""
    try:
        tx_hash = str(log["transactionHash"])
    except KeyError as exc:
        raise ValueError("Infura log has no transactionHash") from exc

    transaction = rpc_client.call("eth_getTransactionByHash", [tx_hash])
    receipt = rpc_client.call("eth_getTransactionReceipt", [tx_hash])
    if not isinstance(transaction, dict) or not isinstance(receipt, dict):
        raise RuntimeError(f"Infura transaction details are unavailable for {tx_hash}")

    block_number_raw = receipt.get("blockNumber", transaction.get("blockNumber"))
    block_number = _hex_int(block_number_raw, field="blockNumber")
    block = rpc_client.call("eth_getBlockByNumber", [hex(block_number), False])
    if not isinstance(block, dict):
        raise RuntimeError(f"Infura block details are unavailable for {block_number}")

    effective_gas_price = receipt.get("effectiveGasPrice", transaction.get("gasPrice"))
    to_address = transaction.get("to")
    if not isinstance(to_address, str):
        raise ValueError("Infura transaction has no to address")

    try:
        return TransactionMessage(
            tx_hash=tx_hash,
            block_number=block_number,
            block_timestamp=_hex_int(block["timestamp"], field="timestamp"),
            from_address=str(transaction["from"]),
            to_address=to_address,
            value_wei=_hex_int(transaction["value"], field="value"),
            gas_price_wei=_hex_int(effective_gas_price, field="effectiveGasPrice"),
            gas_used=_hex_int(receipt["gasUsed"], field="gasUsed"),
            contract_address=contract_address,
            source="realtime",
            ingested_at=ingested_at,
        )
    except KeyError as exc:
        raise ValueError("Infura returned incomplete transaction details") from exc


def publish_message(
    producer: KafkaProducer,
    *,
    topic: str,
    message: TransactionMessage,
) -> None:
    """Publish one keyed message and wait for Kafka delivery acknowledgement."""
    delivery_errors: list[str] = []

    def on_delivery(error: Any, _message: Any) -> None:
        if error is not None:
            delivery_errors.append(str(error))

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


def _process_log(
    log: Mapping[str, Any],
    *,
    settings: Settings,
    producer: KafkaProducer,
    rpc_client: RpcClient,
    seen: SeenTransactions,
    timestamp_factory: Callable[[], str],
) -> bool:
    if log.get("removed") is True:
        return False
    tx_hash = log.get("transactionHash")
    if not isinstance(tx_hash, str):
        raise ValueError("Infura log has no valid transactionHash")
    if tx_hash in seen:
        return False

    message = transaction_from_log(
        log,
        contract_address=settings.contract_address,
        rpc_client=rpc_client,
        ingested_at=timestamp_factory(),
    )
    publish_message(producer, topic=settings.kafka_topic, message=message)
    seen.add(tx_hash)
    logger.info(
        "published realtime transaction %s from block %s",
        message.tx_hash,
        message.block_number,
    )
    return True


def current_block_number(rpc_client: RpcClient) -> int:
    return _hex_int(rpc_client.call("eth_blockNumber"), field="blockNumber")


def poll_for_logs(
    cursor: int,
    *,
    settings: Settings,
    producer: KafkaProducer,
    rpc_client: RpcClient,
    seen: SeenTransactions,
    timestamp_factory: Callable[[], str] = utc_now_iso,
) -> int:
    """Poll all completed blocks after cursor and return the new cursor."""
    latest = current_block_number(rpc_client)
    while cursor < latest:
        end_block = min(cursor + settings.poll_block_range, latest)
        logs = rpc_client.call(
            "eth_getLogs",
            [
                {
                    "fromBlock": hex(cursor + 1),
                    "toBlock": hex(end_block),
                    "address": settings.contract_address,
                }
            ],
        )
        if not isinstance(logs, list) or not all(isinstance(log, dict) for log in logs):
            raise RuntimeError("Infura eth_getLogs returned an invalid result")
        for log in logs:
            _process_log(
                log,
                settings=settings,
                producer=producer,
                rpc_client=rpc_client,
                seen=seen,
                timestamp_factory=timestamp_factory,
            )
        logger.info(
            "polled Infura logs for blocks=[%s, %s], found=%s",
            cursor + 1,
            end_block,
            len(logs),
        )
        cursor = end_block
    return cursor


def _subscription_log(raw_message: str | bytes) -> dict[str, Any] | None:
    try:
        payload = json.loads(raw_message)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("Infura websocket returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("Infura websocket payload must be a JSON object")
    params = payload.get("params")
    if not isinstance(params, dict):
        return None
    result = params.get("result")
    if not isinstance(result, dict):
        return None
    return result


def run_websocket(
    cursor: int,
    *,
    settings: Settings,
    producer: KafkaProducer,
    rpc_client: RpcClient,
    seen: SeenTransactions,
    connect: WebSocketFactory = websocket_connect,
    timestamp_factory: Callable[[], str] = utc_now_iso,
    should_stop: Callable[[], bool] = _is_shutdown,
) -> int:
    """Subscribe to contract logs and process messages until shutdown or a drop."""
    with connect(settings.websocket_url, open_timeout=settings.request_timeout) as websocket:
        websocket.send(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["logs", {"address": settings.contract_address}],
                }
            )
        )
        acknowledgement = json.loads(websocket.recv(timeout=settings.request_timeout))
        if not isinstance(acknowledgement, dict) or acknowledgement.get("error") is not None:
            raise RuntimeError("Infura rejected the logs websocket subscription")
        if not isinstance(acknowledgement.get("result"), str):
            raise RuntimeError("Infura websocket subscription acknowledgement is invalid")

        # The subscription is active before this catch-up poll, so events arriving after
        # the HTTP head snapshot remain buffered on the websocket and cannot be missed.
        cursor = poll_for_logs(
            cursor,
            settings=settings,
            producer=producer,
            rpc_client=rpc_client,
            seen=seen,
            timestamp_factory=timestamp_factory,
        )
        logger.info("subscribed to Infura logs for contract %s", settings.contract_address)

        while not should_stop():
            try:
                raw_message = websocket.recv(timeout=WEBSOCKET_RECEIVE_TIMEOUT_SECONDS)
            except TimeoutError:
                continue
            log = _subscription_log(raw_message)
            if log is None:
                continue
            _process_log(
                log,
                settings=settings,
                producer=producer,
                rpc_client=rpc_client,
                seen=seen,
                timestamp_factory=timestamp_factory,
            )
    return cursor


def _poll_before_reconnect(
    cursor: int,
    *,
    settings: Settings,
    producer: KafkaProducer,
    rpc_client: RpcClient,
    seen: SeenTransactions,
    sleep: Callable[[float], None],
    timestamp_factory: Callable[[], str],
    should_stop: Callable[[], bool],
) -> int:
    remaining = settings.websocket_retry_interval
    while not should_stop():
        try:
            cursor = poll_for_logs(
                cursor,
                settings=settings,
                producer=producer,
                rpc_client=rpc_client,
                seen=seen,
                timestamp_factory=timestamp_factory,
            )
        except Exception as exc:
            logger.error(
                "Infura polling fallback failed; will retry (error_type=%s)",
                type(exc).__name__,
            )

        if should_stop() or remaining <= 0:
            break
        delay = min(settings.poll_interval, remaining)
        sleep(delay)
        remaining -= delay
        if remaining <= 0:
            break
    return cursor


def run_realtime(
    settings: Settings,
    producer: KafkaProducer,
    *,
    rpc_client: RpcClient | None = None,
    connect: WebSocketFactory = websocket_connect,
    sleep: Callable[[float], None] = time.sleep,
    timestamp_factory: Callable[[], str] = utc_now_iso,
    should_stop: Callable[[], bool] = _is_shutdown,
) -> None:
    """Run websocket ingestion, polling while disconnected, and reconnect forever."""
    rpc_client = rpc_client or InfuraRpcClient(settings.http_url, timeout=settings.request_timeout)
    cursor = current_block_number(rpc_client)
    seen = SeenTransactions()
    logger.info("starting realtime ingestion after block %s", cursor)

    while not should_stop():
        try:
            cursor = run_websocket(
                cursor,
                settings=settings,
                producer=producer,
                rpc_client=rpc_client,
                seen=seen,
                connect=connect,
                timestamp_factory=timestamp_factory,
                should_stop=should_stop,
            )
        except Exception as exc:
            if should_stop():
                break
            # Websocket exception text can contain its credential-bearing URL.
            logger.warning(
                "Infura websocket unavailable; falling back to polling (error_type=%s)",
                type(exc).__name__,
            )
            cursor = _poll_before_reconnect(
                cursor,
                settings=settings,
                producer=producer,
                rpc_client=rpc_client,
                seen=seen,
                sleep=sleep,
                timestamp_factory=timestamp_factory,
                should_stop=should_stop,
            )


def _new_producer(settings: Settings) -> Producer:
    return Producer({"bootstrap.servers": settings.kafka_broker})


def main() -> None:
    global _shutdown
    _shutdown = False
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    try:
        settings = Settings.from_env()
    except ValueError as exc:
        logger.error("invalid configuration: %s", exc)
        raise SystemExit(2) from None
    logger.info(
        "config: contract=%s poll_interval=%s kafka_broker=%s kafka_topic=%s",
        settings.contract_address,
        settings.poll_interval,
        settings.kafka_broker,
        settings.kafka_topic,
    )
    run_realtime(settings, _new_producer(settings))


if __name__ == "__main__":
    main()
