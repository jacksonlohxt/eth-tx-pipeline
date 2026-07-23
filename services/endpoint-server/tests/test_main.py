from dataclasses import fields

import mongomock
import pytest
from eth_tx_shared.schema import EnrichedTransaction
from fastapi.testclient import TestClient
from src.main import (
    EnrichedTransactionResponse,
    Settings,
    app,
    get_transactions_collection,
)

ADDRESS_A = "0x1111111111111111111111111111111111111111"
ADDRESS_B = "0x2222222222222222222222222222222222222222"
CONTRACT = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"


def _transaction(
    tx_hash: str,
    *,
    block_number: int,
    block_timestamp: int,
    from_address: str,
    to_address: str,
) -> dict:
    return EnrichedTransaction(
        tx_hash=tx_hash,
        block_number=block_number,
        block_timestamp=block_timestamp,
        from_address=from_address,
        to_address=to_address,
        value_wei=0,
        gas_price_wei=20_000_000_000,
        gas_used=150_000,
        contract_address=CONTRACT,
        source="historical",
        ingested_at="2026-07-13T00:00:00Z",
        fee_eth=0.003,
        fee_usd=9.87,
        eth_usd_exchange_rate=3_290.0,
        enriched_at="2026-07-13T00:00:05Z",
    ).to_mongo_document()


@pytest.fixture
def collection():
    mongo_client = mongomock.MongoClient("mongodb://localhost:27017/eth_tx_pipeline")
    transactions = mongo_client.get_default_database()["transactions"]
    transactions.insert_many(
        [
            _transaction(
                "0x01",
                block_number=100,
                block_timestamp=1_000,
                from_address=ADDRESS_A,
                to_address=CONTRACT,
            ),
            _transaction(
                "0x02",
                block_number=101,
                block_timestamp=1_100,
                from_address=ADDRESS_B,
                to_address=ADDRESS_A,
            ),
            _transaction(
                "0x03",
                block_number=102,
                block_timestamp=1_200,
                from_address=ADDRESS_B,
                to_address=CONTRACT,
            ),
            _transaction(
                "0x04",
                block_number=103,
                block_timestamp=1_300,
                from_address=ADDRESS_A,
                to_address=CONTRACT,
            ),
        ]
    )
    yield transactions
    mongo_client.close()


@pytest.fixture
def client(collection):
    app.dependency_overrides[get_transactions_collection] = lambda: collection
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def _ids(response) -> list[str]:
    assert response.status_code == 200
    return [item["_id"] for item in response.json()["items"]]


def test_health_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_transactions_returns_seeded_enriched_documents(client):
    response = client.get("/transactions")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 4
    assert body["offset"] == 0
    assert body["limit"] == 50
    assert [item["_id"] for item in body["items"]] == ["0x01", "0x02", "0x03", "0x04"]
    assert body["items"][0] == {
        "_id": "0x01",
        "tx_hash": "0x01",
        "block_number": 100,
        "block_timestamp": 1_000,
        "from_address": ADDRESS_A,
        "to_address": CONTRACT,
        "value_wei": 0,
        "gas_price_wei": 20_000_000_000,
        "gas_used": 150_000,
        "contract_address": CONTRACT,
        "source": "historical",
        "ingested_at": "2026-07-13T00:00:00Z",
        "fee_eth": 0.003,
        "fee_usd": 9.87,
        "eth_usd_exchange_rate": 3_290.0,
        "enriched_at": "2026-07-13T00:00:05Z",
    }


@pytest.mark.parametrize(
    ("query", "expected_ids"),
    [
        ({"address": ADDRESS_A}, ["0x01", "0x02", "0x04"]),
        ({"block_number_from": 101, "block_number_to": 102}, ["0x02", "0x03"]),
        ({"timestamp_from": 1_100, "timestamp_to": 1_200}, ["0x02", "0x03"]),
        (
            {"address": ADDRESS_A, "block_number_from": 101, "timestamp_to": 1_200},
            ["0x02"],
        ),
    ],
)
def test_transactions_filters(client, query, expected_ids):
    response = client.get("/transactions", params=query)

    assert _ids(response) == expected_ids
    assert response.json()["total"] == len(expected_ids)


def test_transactions_uses_deterministic_offset_limit_pagination(client):
    first_page = client.get("/transactions", params={"offset": 0, "limit": 2})
    second_page = client.get("/transactions", params={"offset": 2, "limit": 2})

    assert _ids(first_page) == ["0x01", "0x02"]
    assert _ids(second_page) == ["0x03", "0x04"]
    assert first_page.json()["total"] == second_page.json()["total"] == 4


def test_transactions_rejects_inverted_ranges(client):
    response = client.get(
        "/transactions",
        params={"block_number_from": 102, "block_number_to": 101},
    )

    assert response.status_code == 422
    assert "block_number_from" in response.json()["detail"]


def test_transactions_appears_in_openapi_schema(client):
    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/transactions"]["get"]

    assert operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/TransactionPage"
    }
    assert {parameter["name"] for parameter in operation["parameters"]} == {
        "address",
        "block_number_from",
        "block_number_to",
        "timestamp_from",
        "timestamp_to",
        "offset",
        "limit",
    }
    enriched_schema = schema["components"]["schemas"]["EnrichedTransactionResponse"]
    assert {"_id", "fee_eth", "fee_usd", "eth_usd_exchange_rate"} <= set(
        enriched_schema["properties"]
    )


def test_response_model_tracks_shared_enriched_transaction_schema():
    response_fields = {
        field.alias or name
        for name, field in EnrichedTransactionResponse.model_fields.items()
    }
    shared_fields = {field.name for field in fields(EnrichedTransaction)}

    assert response_fields == shared_fields | {"_id"}


def test_settings_default_mongodb_url_includes_database_path():
    assert Settings.from_env({}).mongodb_url == "mongodb://localhost:27017/eth_tx_pipeline"


def test_transaction_with_wei_beyond_bson_int64_round_trips_as_integer(collection):
    """Regression for Defect B: Mongo stores value_wei/gas_price_wei as strings
    (BSON caps ints at signed int64), but the API must still return integers.
    """
    huge_value_wei = 2**70
    huge_gas_price_wei = 2**64
    document = EnrichedTransaction(
        tx_hash="0xoverflow",
        block_number=200,
        block_timestamp=2_000,
        from_address=ADDRESS_A,
        to_address=CONTRACT,
        value_wei=huge_value_wei,
        gas_price_wei=huge_gas_price_wei,
        gas_used=150_000,
        contract_address=CONTRACT,
        source="realtime",
        ingested_at="2026-07-13T00:00:00Z",
        fee_eth=0.003,
        fee_usd=9.87,
        eth_usd_exchange_rate=3_290.0,
        enriched_at="2026-07-13T00:00:05Z",
    ).to_mongo_document()
    assert document["value_wei"] == str(huge_value_wei)
    collection.insert_one(document)

    app.dependency_overrides[get_transactions_collection] = lambda: collection
    try:
        with TestClient(app) as test_client:
            response = test_client.get("/transactions", params={"block_number_from": 200})
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    assert items[0]["value_wei"] == huge_value_wei
    assert items[0]["gas_price_wei"] == huge_gas_price_wei
