# Project agent memory

This file is the project's committed home for project-intrinsic agent knowledge: build, test, release, architecture, and sharp-edge notes that should travel with the code.

- Add durable project-specific notes here as they are discovered through real work.
- Architecture and message/document contracts: `docs/architecture.md`, `docs/message-schema.md`. Read these before touching any service - they are the contract other services depend on.
- Every service in `services/*/` depends on `shared/eth_tx_shared` for the schema (`pip install -e shared/` before installing a service's own `requirements.txt`). Docker builds use repo root as build context (`docker-compose.yml` sets `context: .`, `dockerfile: services/<name>/Dockerfile`) so each Dockerfile can `COPY shared /app/shared`.
- Test/lint a single service: `cd services/<name> && pip install -r ../../requirements-dev.txt && pip install -e ../../shared && pip install -r requirements.txt && pytest`. Lint everything with `ruff check services shared` from the repo root (ruff config is in the root `pyproject.toml`).
- This scaffold was built in an environment without Docker installed, so `docker-compose.yml` was validated by parsing it as YAML only - it has never had a live `docker compose up` run against it. Treat the first real `docker compose up` in this project as unverified until someone actually does it.
- `message-consumer` and `db-indexing-sidecar` are implemented. The producer services and endpoint-server data API remain intentional stubs for later increments; see `README.md` for current status.

## Maintaining this file

Keep this file for knowledge useful to almost every future agent session in this project.
Do not repeat what the codebase already shows; point to the authoritative file or command instead.
Prefer rewriting or pruning existing entries over appending new ones.
When updating this file, preserve this bar for all agents and keep entries concise.
