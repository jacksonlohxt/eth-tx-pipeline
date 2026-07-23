import dataclasses

from eth_tx_shared.schema import EnrichedTransaction, TransactionMessage

SAMPLE_MESSAGE = TransactionMessage(
    tx_hash="0xabc",
    block_number=18_000_000,
    block_timestamp=1_700_000_000,
    from_address="0xfrom",
    to_address="0xto",
    value_wei=0,
    gas_price_wei=20_000_000_000,
    gas_used=150_000,
    contract_address="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
    source="historical",
    ingested_at="2026-07-13T00:00:00Z",
)


def test_transaction_message_json_roundtrip():
    raw = SAMPLE_MESSAGE.to_json()
    restored = TransactionMessage.from_json(raw)
    assert restored == SAMPLE_MESSAGE


def test_enriched_transaction_from_message():
    enriched = EnrichedTransaction.from_message(
        SAMPLE_MESSAGE,
        fee_eth=0.003,
        fee_usd=9.87,
        eth_usd_exchange_rate=3290.0,
        enriched_at="2026-07-13T00:00:05Z",
    )
    doc = enriched.to_mongo_document()
    assert doc["_id"] == SAMPLE_MESSAGE.tx_hash
    assert doc["fee_eth"] == 0.003
    assert doc["gas_used"] == SAMPLE_MESSAGE.gas_used
    assert doc["value_wei"] == str(SAMPLE_MESSAGE.value_wei)
    assert doc["gas_price_wei"] == str(SAMPLE_MESSAGE.gas_price_wei)


def test_to_mongo_document_serializes_wei_beyond_bson_int64_as_string():
    """BSON caps ints at signed int64 (2^63-1); this value is ~2^70."""
    huge_value_wei = 2**70
    message = dataclasses.replace(SAMPLE_MESSAGE, value_wei=huge_value_wei)
    enriched = EnrichedTransaction.from_message(
        message,
        fee_eth=0.003,
        fee_usd=9.87,
        eth_usd_exchange_rate=3290.0,
        enriched_at="2026-07-13T00:00:05Z",
    )
    doc = enriched.to_mongo_document()
    assert doc["value_wei"] == str(huge_value_wei)
    assert int(doc["value_wei"]) == huge_value_wei
