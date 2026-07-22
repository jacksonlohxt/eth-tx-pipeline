from collections import deque

import mongomock
import pytest
from eth_tx_shared.schema import TransactionMessage
from src.main import Settings, consume_messages


class FakeKafkaMessage:
    def __init__(self, value: bytes):
        self._value = value

    def value(self) -> bytes:
        return self._value

    def error(self):
        return None


class InMemoryKafka:
    def __init__(self):
        self.messages = deque()
        self.committed = []

    def publish(self, message: TransactionMessage) -> None:
        self.messages.append(FakeKafkaMessage(message.to_json().encode()))

    def poll(self, timeout: float):
        del timeout
        return self.messages.popleft() if self.messages else None

    def commit(self, *, message: FakeKafkaMessage, asynchronous: bool) -> None:
        assert asynchronous is False
        self.committed.append(message)


def test_transaction_flows_from_kafka_through_enrichment_to_mongo_upsert():
    transaction = TransactionMessage(
        tx_hash="0xabc123",
        block_number=18_000_042,
        block_timestamp=1_693_526_400,
        from_address="0x1111111111111111111111111111111111111111",
        to_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        value_wei=0,
        gas_price_wei=20_000_000_000,
        gas_used=150_000,
        contract_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
        source="historical",
        ingested_at="2026-07-13T00:00:00Z",
    )
    exchange_rate = 3_290.0
    enriched_at = "2026-07-13T00:00:05Z"
    kafka = InMemoryKafka()
    mongo_client = mongomock.MongoClient("mongodb://localhost:27017/eth_tx_pipeline")
    collection = mongo_client.get_default_database()["transactions"]

    kafka.publish(transaction)
    assert (
        consume_messages(
            kafka,
            collection,
            eth_usd_exchange_rate=exchange_rate,
            timestamp_factory=lambda: enriched_at,
            max_messages=1,
        )
        == 1
    )

    expected_fee_eth = transaction.gas_price_wei * transaction.gas_used / 1e18
    document = collection.find_one({"_id": transaction.tx_hash})
    assert document == {
        "_id": transaction.tx_hash,
        "tx_hash": transaction.tx_hash,
        "block_number": transaction.block_number,
        "block_timestamp": transaction.block_timestamp,
        "from_address": transaction.from_address,
        "to_address": transaction.to_address,
        "value_wei": transaction.value_wei,
        "gas_price_wei": transaction.gas_price_wei,
        "gas_used": transaction.gas_used,
        "contract_address": transaction.contract_address,
        "source": transaction.source,
        "ingested_at": transaction.ingested_at,
        "fee_eth": pytest.approx(expected_fee_eth),
        "fee_usd": pytest.approx(expected_fee_eth * exchange_rate),
        "eth_usd_exchange_rate": exchange_rate,
        "enriched_at": enriched_at,
    }

    kafka.publish(transaction)
    consume_messages(
        kafka,
        collection,
        eth_usd_exchange_rate=exchange_rate,
        timestamp_factory=lambda: "2026-07-13T00:01:00Z",
        max_messages=1,
    )

    assert collection.count_documents({}) == 1
    assert collection.find_one({"_id": transaction.tx_hash})["enriched_at"] == (
        "2026-07-13T00:01:00Z"
    )
    assert len(kafka.committed) == 2


def test_settings_default_mongodb_url_includes_database_path():
    settings = Settings.from_env({})

    assert settings.mongodb_url == "mongodb://localhost:27017/eth_tx_pipeline"
    assert settings.eth_usd_exchange_rate == 3_000.0


def test_settings_rejects_invalid_exchange_rate():
    with pytest.raises(ValueError, match="positive finite"):
        Settings.from_env({"ETH_USD_EXCHANGE_RATE": "0"})
