import io
import json
import logging
import urllib.parse
from pathlib import Path

from eth_tx_shared.schema import TransactionMessage
from src.main import Settings, run_historical_backfill


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


def settings(*, api_key: str = "") -> Settings:
    return Settings(
        contract_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        first_block=18_000_042,
        last_block=18_000_042,
        batch_size=100,
        etherscan_api_key=api_key,
        kafka_broker="in-memory:9092",
        kafka_topic="transactions",
    )


def test_fixture_mode_emits_schema_valid_transaction_messages_to_in_process_kafka(
    caplog,
):
    kafka = InMemoryKafkaProducer()

    def fail_on_network(*_args, **_kwargs):
        raise AssertionError("fixture mode must not make a network request")

    with caplog.at_level(logging.INFO):
        published = run_historical_backfill(
            settings(),
            kafka,
            urlopen=fail_on_network,
            timestamp_factory=lambda: "2026-07-13T00:00:00Z",
        )

    assert published == len(kafka.records)
    assert published >= 1
    assert "using credential-free recorded fixture mode" in caplog.text
    assert kafka.flush_calls

    for record in kafka.records:
        message = TransactionMessage.from_json(record["value"])
        assert record["topic"] == "transactions"
        assert record["key"] == message.tx_hash
        assert message.source == "historical"
        assert message.contract_address == settings().contract_address
        assert isinstance(message.block_number, int)
        assert isinstance(message.block_timestamp, int)
        assert isinstance(message.value_wei, int)
        assert isinstance(message.gas_price_wei, int)
        assert isinstance(message.gas_used, int)


def test_real_mode_fetches_mocked_etherscan_response_and_publishes_without_network(
    caplog,
):
    payload = {
        "status": "1",
        "message": "OK",
        "result": [
            {
                "blockNumber": "18000042",
                "timeStamp": "1693526400",
                "hash": "0xabc123",
                "from": "0x1111111111111111111111111111111111111111",
                "to": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "value": "0",
                "gasPrice": "20000000000",
                "gasUsed": "150000",
            }
        ],
    }
    requested = []

    def fake_urlopen(url, *, timeout):
        requested.append((url, timeout))
        return io.BytesIO(json.dumps(payload).encode())

    kafka = InMemoryKafkaProducer()
    live_settings = settings(api_key="test-api-key")
    with caplog.at_level(logging.INFO):
        published = run_historical_backfill(
            live_settings,
            kafka,
            urlopen=fake_urlopen,
            timestamp_factory=lambda: "2026-07-13T00:00:00Z",
        )

    assert published == 1
    assert "using live Etherscan API mode" in caplog.text
    assert len(requested) == 1
    url, timeout = requested[0]
    query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    assert urllib.parse.urlparse(url).path == "/v2/api"
    assert query == {
        "chainid": ["1"],
        "module": ["account"],
        "action": ["txlist"],
        "address": [live_settings.contract_address],
        "startblock": ["18000042"],
        "endblock": ["18000042"],
        "page": ["1"],
        "offset": ["10000"],
        "sort": ["asc"],
        "apikey": ["test-api-key"],
    }
    assert timeout > 0

    message = TransactionMessage.from_json(kafka.records[0]["value"])
    assert message == TransactionMessage(
        tx_hash="0xabc123",
        block_number=18_000_042,
        block_timestamp=1_693_526_400,
        from_address="0x1111111111111111111111111111111111111111",
        to_address="0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
        value_wei=0,
        gas_price_wei=20_000_000_000,
        gas_used=150_000,
        contract_address=live_settings.contract_address,
        source="historical",
        ingested_at="2026-07-13T00:00:00Z",
    )


def test_real_mode_walks_the_inclusive_block_range_in_nonoverlapping_batches():
    requested_ranges = []

    def fake_urlopen(url, *, timeout):
        del timeout
        query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        start_block = int(query["startblock"][0])
        end_block = int(query["endblock"][0])
        requested_ranges.append((start_block, end_block))
        payload = {
            "status": "1",
            "message": "OK",
            "result": [
                {
                    "blockNumber": str(start_block),
                    "timeStamp": "1693526400",
                    "hash": f"0x{start_block}",
                    "from": "0x1111111111111111111111111111111111111111",
                    "to": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                    "value": "0",
                    "gasPrice": "20000000000",
                    "gasUsed": "150000",
                }
            ],
        }
        return io.BytesIO(json.dumps(payload).encode())

    live_settings = Settings(
        contract_address=settings().contract_address,
        first_block=100,
        last_block=104,
        batch_size=2,
        etherscan_api_key="test-api-key",
        kafka_broker="in-memory:9092",
        kafka_topic="transactions",
    )
    kafka = InMemoryKafkaProducer()

    assert run_historical_backfill(live_settings, kafka, urlopen=fake_urlopen) == 3
    assert requested_ranges == [(100, 101), (102, 103), (104, 104)]


def test_settings_default_to_fixture_mode_and_bundled_fixture_exists():
    loaded = Settings.from_env({})

    assert loaded.etherscan_api_key == ""
    assert loaded.fixture_path == Path(__file__).parent / "fixtures/etherscan_txlist.json"
    assert loaded.fixture_path.is_file()
