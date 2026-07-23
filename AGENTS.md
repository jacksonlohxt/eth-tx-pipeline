# Project agent memory

This file is the project's committed home for project-intrinsic agent knowledge: build, test, release, architecture, and sharp-edge notes that should travel with the code.

- Add durable project-specific notes here as they are discovered through real work.
- Architecture and message/document contracts: `docs/architecture.md`, `docs/message-schema.md`. Read these before touching any service - they are the contract other services depend on.
- Every service in `services/*/` depends on `shared/eth_tx_shared` for the schema (`pip install -e shared/` before installing a service's own `requirements.txt`). Docker builds use repo root as build context (`docker-compose.yml` sets `context: .`, `dockerfile: services/<name>/Dockerfile`) so each Dockerfile can `COPY shared /app/shared`.
- Test/lint a single service: `cd services/<name> && pip install -r ../../requirements-dev.txt && pip install -e ../../shared && pip install -r requirements.txt && pytest`. Lint everything with `ruff check services shared` from the repo root (ruff config is in the root `pyproject.toml`).
- The full stack has been validated with a live `docker compose up` (all 8 containers healthy, fixture backfill flowing source->Kafka->consumer->Mongo->API). Re-run/regress it with `scripts/e2e_smoke.sh` (also the CI `e2e` job); it needs only Docker + `python3`. On a machine without a container runtime (e.g. bare macOS), install one first - `colima` provides the Linux VM `docker` needs.
- Kafka advertises two listeners: `INTERNAL` (`kafka:9092`) for in-network services and `EXTERNAL` (`localhost:29092`, the published port) for host-side tooling. Point host Kafka clients at `localhost:29092`, not `9092`.
- All five services are implemented. `transactions-realtime` requires `INFURA_PROJECT_ID` at runtime; its websocket, polling fallback, and reconnect paths are covered without live credentials by mocked tests in `services/transactions-realtime/tests/`.
- `value_wei`/`gas_price_wei` are stored in MongoDB as decimal strings, not raw ints - BSON caps ints at signed int64 and real mainnet values exceed it. This was invisible to the fixture/mocked test suite (which only exercises `value_wei=0`) and only surfaced against live credentials; see `shared/eth_tx_shared/schema.py` (`MONGO_LARGE_INT_FIELDS`, `to_mongo_document`) and `services/endpoint-server/src/main.py` (`EnrichedTransactionResponse` parses them back to int - the API contract is still numeric). Kafka's `TransactionMessage` JSON is unaffected. Same lesson applies broadly: fixture/mocked tests here use small/zero values and can miss magnitude-dependent bugs - live-credential testing is the only thing that catches them.
- `message-consumer`'s `consume_messages` treats `UNKNOWN_TOPIC_OR_PART` as retriable (same as `_PARTITION_EOF`), not fatal - a consumer starting before any producer has created the topic is a normal cold-start race, not an error.
- Compose publishes `8000`/`27017`/`29092` bound to `127.0.0.1` only (not `0.0.0.0`) - keep new published ports loopback-bound too unless there's a specific reason for LAN exposure.

## Maintaining this file

Keep this file for knowledge useful to almost every future agent session in this project.
Do not repeat what the codebase already shows; point to the authoritative file or command instead.
Prefer rewriting or pruning existing entries over appending new ones.
When updating this file, preserve this bar for all agents and keep entries concise.
