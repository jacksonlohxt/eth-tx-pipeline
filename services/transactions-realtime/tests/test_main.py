import json

import pytest
from eth_tx_shared.schema import TransactionMessage
from src.main import SeenTransactions, Settings, run_realtime, run_websocket

CONTRACT = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
SENDER = "0x1111111111111111111111111111111111111111"


class InMemoryKafkaProducer:
    def __init__(self):
        self.records = []
        self.flush_calls = []

    def produce(self, topic, *, key, value, on_delivery):
        self.records.append({"topic": topic, "key": key, "value": value})
        on_delivery(None, self.records[-1])

    def poll(self, timeout):
        del timeout
        return 0

    def flush(self, timeout):
        self.flush_calls.append(timeout)
        return 0


class FakeRpcClient:
    def __init__(self, block_numbers, polled_logs=None):
        self.block_numbers = iter(block_numbers)
        self.polled_logs = list(polled_logs or [])
        self.calls = []

    def call(self, method, params=None):
        self.calls.append((method, params or []))
        if method == "eth_blockNumber":
            return next(self.block_numbers)
        if method == "eth_getLogs":
            return self.polled_logs.pop(0) if self.polled_logs else []
        if method == "eth_getTransactionByHash":
            tx_hash = params[0]
            suffix = int(tx_hash[-1], 16)
            return {
                "hash": tx_hash,
                "blockNumber": "0x65",
                "from": SENDER,
                "to": CONTRACT.lower(),
                "value": hex(suffix),
                "gasPrice": "0x4a817c800",
            }
        if method == "eth_getTransactionReceipt":
            return {
                "blockNumber": "0x65",
                "effectiveGasPrice": "0x4a817c801",
                "gasUsed": "0x249f0",
            }
        if method == "eth_getBlockByNumber":
            return {"number": params[0], "timestamp": "0x64ed1c00"}
        raise AssertionError(f"unexpected JSON-RPC method: {method}")


class FakeWebSocket:
    def __init__(self, messages):
        self.messages = iter(messages)
        self.sent = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def send(self, message):
        self.sent.append(json.loads(message))

    def recv(self, timeout=None):
        del timeout
        return next(self.messages)


def settings(**overrides):
    values = {
        "contract_address": CONTRACT,
        "infura_project_id": "mock-project-id",
        "kafka_broker": "in-memory:9092",
        "kafka_topic": "transactions",
        "poll_interval": 0.01,
        "websocket_retry_interval": 0.01,
    }
    values.update(overrides)
    return Settings(**values)


def subscription_event(tx_hash):
    return json.dumps(
        {
            "jsonrpc": "2.0",
            "method": "eth_subscription",
            "params": {
                "subscription": "0xsubscription",
                "result": {
                    "address": CONTRACT,
                    "blockNumber": "0x65",
                    "transactionHash": tx_hash,
                    "removed": False,
                },
            },
        }
    )


def assert_realtime_message(record, tx_hash):
    message = TransactionMessage.from_json(record["value"])
    assert record["topic"] == "transactions"
    assert record["key"] == tx_hash
    assert message == TransactionMessage(
        tx_hash=tx_hash,
        block_number=101,
        block_timestamp=1_693_260_800,
        from_address=SENDER,
        to_address=CONTRACT.lower(),
        value_wei=int(tx_hash[-1], 16),
        gas_price_wei=20_000_000_001,
        gas_used=150_000,
        contract_address=CONTRACT,
        source="realtime",
        ingested_at="2026-07-13T00:00:00Z",
    )


def test_mocked_websocket_stream_publishes_schema_valid_realtime_messages():
    tx_hashes = ["0xabc1", "0xabc2"]
    websocket = FakeWebSocket(
        [
            json.dumps({"jsonrpc": "2.0", "id": 1, "result": "0xsubscription"}),
            *(subscription_event(tx_hash) for tx_hash in tx_hashes),
        ]
    )
    rpc = FakeRpcClient(["0x64"])
    kafka = InMemoryKafkaProducer()

    cursor = run_websocket(
        100,
        settings=settings(),
        producer=kafka,
        rpc_client=rpc,
        seen=SeenTransactions(),
        connect=lambda *_args, **_kwargs: websocket,
        timestamp_factory=lambda: "2026-07-13T00:00:00Z",
        should_stop=lambda: len(kafka.records) == len(tx_hashes),
    )

    assert cursor == 100
    assert websocket.sent == [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": ["logs", {"address": CONTRACT}],
        }
    ]
    assert len(kafka.records) == 2
    assert len(kafka.flush_calls) == 2
    for record, tx_hash in zip(kafka.records, tx_hashes, strict=True):
        assert_realtime_message(record, tx_hash)


def test_websocket_failure_falls_back_to_mocked_log_polling(caplog):
    tx_hash = "0xabc3"
    rpc = FakeRpcClient(
        ["0x64", "0x65"],
        polled_logs=[
            [
                {
                    "address": CONTRACT,
                    "blockNumber": "0x65",
                    "transactionHash": tx_hash,
                    "removed": False,
                }
            ]
        ],
    )
    kafka = InMemoryKafkaProducer()
    websocket_attempts = []

    def failed_connect(*args, **kwargs):
        websocket_attempts.append((args, kwargs))
        raise OSError("mock websocket drop")

    def fail_on_sleep(_seconds):
        raise AssertionError("the test should stop immediately after the polled message")

    run_realtime(
        settings(),
        kafka,
        rpc_client=rpc,
        connect=failed_connect,
        sleep=fail_on_sleep,
        timestamp_factory=lambda: "2026-07-13T00:00:00Z",
        should_stop=lambda: len(kafka.records) == 1,
    )

    assert len(websocket_attempts) == 1
    assert "falling back to polling" in caplog.text
    expected_poll = (
        "eth_getLogs",
        [{"fromBlock": "0x65", "toBlock": "0x65", "address": CONTRACT}],
    )
    assert expected_poll in rpc.calls
    assert_realtime_message(kafka.records[0], tx_hash)


def test_polling_fallback_retries_the_websocket_connection():
    tx_hash = "0xabc4"
    rpc = FakeRpcClient(["0x64", "0x64", "0x64"])
    kafka = InMemoryKafkaProducer()
    websocket = FakeWebSocket(
        [
            json.dumps({"jsonrpc": "2.0", "id": 1, "result": "0xsubscription"}),
            subscription_event(tx_hash),
        ]
    )
    attempts = 0

    def connect_after_failure(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("mock initial connection failure")
        return websocket

    run_realtime(
        settings(),
        kafka,
        rpc_client=rpc,
        connect=connect_after_failure,
        sleep=lambda _seconds: None,
        timestamp_factory=lambda: "2026-07-13T00:00:00Z",
        should_stop=lambda: len(kafka.records) == 1,
    )

    assert attempts == 2
    assert_realtime_message(kafka.records[0], tx_hash)


def test_settings_require_an_infura_project_id_without_making_network_calls():
    with pytest.raises(ValueError, match="INFURA_PROJECT_ID must be set"):
        Settings.from_env({})
