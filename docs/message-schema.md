# Message & document schema

This is the contract between services: producers (`transactions-historical`,
`transactions-realtime`) and the consumer (`message-consumer`) must agree on
the exact JSON shape below. The canonical, importable definition lives in
[`shared/eth_tx_shared/schema.py`](../shared/eth_tx_shared/schema.py) as the
dataclasses `TransactionMessage` and `EnrichedTransaction`.

## Why a shared package instead of duplicated dataclasses

Each service could define its own copy of these fields instead of depending
on a shared package. We chose a shared installable package
(`eth-tx-shared`) because:

- **Pro (chosen):** a field rename or addition happens in one place and
  every service that imports it gets a type error at the call site instead
  of a silent runtime mismatch across a Kafka boundary. `to_json` /
  `from_json` / `to_mongo_document` helpers live in one place too.
- **Con (accepted):** every service's Dockerfile has an extra build step
  (`pip install -e shared/`) and its build context must be the repo root
  instead of just the service directory. Cross-service coupling is real,
  but for a schema that is meant to be identical everywhere, we judged the
  coupling worth the safety. If a service ever needs a genuinely different
  shape, it should define its own type rather than bending this shared one.

## `TransactionMessage` - the Kafka topic payload

Published as a UTF-8 JSON string, one message per transaction, on the topic
named by `KAFKA_TOPIC`.

| Field | Type | Description |
|---|---|---|
| `tx_hash` | string | `0x`-prefixed transaction hash. |
| `block_number` | integer | Block the transaction was included in. |
| `block_timestamp` | integer | Unix seconds (UTC) of that block. |
| `from_address` | string | `0x`-prefixed sender address. |
| `to_address` | string | `0x`-prefixed recipient address (the contract, for a call into it). |
| `value_wei` | integer | ETH value transferred, in wei. |
| `gas_price_wei` | integer | Effective gas price paid, in wei. |
| `gas_used` | integer | Gas actually consumed by the transaction. |
| `contract_address` | string | The pool/contract this pipeline tracks (`ETHERSCAN_CONTRACT_ADDRESS`). |
| `source` | string | `"historical"` or `"realtime"` - which producer emitted it. |
| `ingested_at` | string | ISO-8601 UTC timestamp of when the producer ingested it. |

Example:

```json
{
  "tx_hash": "0x9a1c...ef02",
  "block_number": 18000042,
  "block_timestamp": 1693526400,
  "from_address": "0x1111111111111111111111111111111111111111",
  "to_address": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
  "value_wei": 0,
  "gas_price_wei": 20000000000,
  "gas_used": 150000,
  "contract_address": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
  "source": "historical",
  "ingested_at": "2026-07-13T00:00:00Z"
}
```

## `EnrichedTransaction` - the MongoDB document

Written by `message-consumer` to the `transactions` collection. Embeds every
`TransactionMessage` field plus:

| Field | Type | Description |
|---|---|---|
| `fee_eth` | float | `gas_price_wei * gas_used / 1e18`, the transaction fee in ETH. |
| `fee_usd` | float | `fee_eth * eth_usd_exchange_rate`. |
| `eth_usd_exchange_rate` | float | The ETH/USD rate used for this conversion, captured at enrichment time so historical fee_usd values stay reproducible even as rates change. |
| `enriched_at` | string | ISO-8601 UTC timestamp of when `message-consumer` wrote the document. |

The document's `_id` is set to `tx_hash`, making writes naturally
idempotent under at-least-once Kafka delivery (re-processing the same
transaction overwrites the same document instead of duplicating it).

**`value_wei` and `gas_price_wei` are stored as decimal strings**, not BSON
ints: BSON caps integers at signed int64 (2^63-1), which real mainnet wei
values can exceed (`EnrichedTransaction.to_mongo_document`,
`MONGO_LARGE_INT_FIELDS` in `shared/eth_tx_shared/schema.py`). The Kafka
`TransactionMessage` payload is unaffected (`json.dumps` handles
arbitrary-precision ints), and `endpoint-server`'s `/transactions` response
parses these two fields back to JSON integers, so the wire/API contract stays
numeric end to end - only the raw Mongo document uses strings.

Example (as stored in Mongo):

```json
{
  "_id": "0x9a1c...ef02",
  "tx_hash": "0x9a1c...ef02",
  "block_number": 18000042,
  "block_timestamp": 1693526400,
  "from_address": "0x1111111111111111111111111111111111111111",
  "to_address": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
  "value_wei": "0",
  "gas_price_wei": "20000000000",
  "gas_used": 150000,
  "contract_address": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
  "source": "historical",
  "ingested_at": "2026-07-13T00:00:00Z",
  "fee_eth": 0.003,
  "fee_usd": 9.87,
  "eth_usd_exchange_rate": 3290.0,
  "enriched_at": "2026-07-13T00:00:05Z"
}
```
