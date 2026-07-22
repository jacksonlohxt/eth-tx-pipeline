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
