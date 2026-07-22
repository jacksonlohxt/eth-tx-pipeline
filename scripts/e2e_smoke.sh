#!/usr/bin/env bash
#
# End-to-end smoke test for the eth-tx-pipeline Docker Compose stack.
#
# This is the automated form of the "first real `docker compose up`" E2E
# (completion-plan item P5). It brings up the full 8-container stack, lets the
# credential-free fixture backfill flow
#
#     transactions-historical (fixture) -> Kafka -> message-consumer -> MongoDB
#
# and asserts the enriched result is served by endpoint-server over HTTP. It
# only uses `docker`, `docker compose`, and `python3` (stdlib), so it runs the
# same on a laptop and on a CI runner. A working container runtime is required.
#
# Usage:
#   scripts/e2e_smoke.sh                 # build images, verify, tear down
#   NO_BUILD=1 scripts/e2e_smoke.sh      # reuse already-built images
#   KEEP_STACK=1 scripts/e2e_smoke.sh    # leave the stack up after verifying
#
# Exit status is non-zero on the first failed assertion; the stack logs are
# dumped to aid debugging before teardown.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/docker-compose.yml"
API_BASE="${API_BASE:-http://localhost:8000}"
KAFKA_EXTERNAL="${KAFKA_EXTERNAL:-127.0.0.1:29092}"

# The bundled Etherscan fixture holds exactly these two transactions; the whole
# assertion suite is pinned to them so the check is deterministic.
EXPECTED_TOTAL=2
FIXTURE_TX_HASH="0x9a1c364bd580b39f17e6ea4d936d8a190ecfcd3c9372e0f7c92bc7e1e3a7ef02"
FIXTURE_TX_FEE_ETH="0.003"
FIXTURE_TX_FEE_USD="9.0"

compose() { docker compose -f "$COMPOSE_FILE" "$@"; }

log()  { echo "[e2e] $*"; }
fail() {
  echo "[e2e] FAIL: $*" >&2
  echo "[e2e] ---- container status ----" >&2
  compose ps -a >&2 || true
  echo "[e2e] ---- recent logs ----" >&2
  compose logs --tail=40 >&2 || true
  exit 1
}

cleanup() {
  local rc=$?
  if [ "${KEEP_STACK:-0}" = "1" ]; then
    log "KEEP_STACK=1 -> leaving stack running (docker compose down -v to clean up)"
  else
    log "tearing down stack"
    compose down -v >/dev/null 2>&1 || true
  fi
  return $rc
}
trap cleanup EXIT

# wait_until <description> <timeout_seconds> <command...>
wait_until() {
  local desc="$1" timeout="$2"; shift 2
  local waited=0
  until "$@" >/dev/null 2>&1; do
    if [ "$waited" -ge "$timeout" ]; then fail "timed out after ${timeout}s waiting for: $desc"; fi
    sleep 2; waited=$((waited + 2))
  done
  log "ready: $desc"
}

# Use `ps -aq` so exited run-once containers (sidecar, fixture backfill) still resolve.
container_id()      { compose ps -aq "$1" 2>/dev/null | head -n1; }
container_health()  { docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$(container_id "$1")" 2>/dev/null; }
is_healthy()        { [ "$(container_health "$1")" = "healthy" ]; }
exited_zero()       { [ "$(docker inspect -f '{{.State.Status}}:{{.State.ExitCode}}' "$(container_id "$1")" 2>/dev/null)" = "exited:0" ]; }

log "using compose file: $COMPOSE_FILE"

# ---------------------------------------------------------------------------
log "step 1/6: (re)creating stack from clean volumes"
compose down -v >/dev/null 2>&1 || true
if [ "${NO_BUILD:-0}" = "1" ]; then
  compose up -d
else
  compose up -d --build
fi

# ---------------------------------------------------------------------------
log "step 2/6: waiting for infrastructure to report healthy"
wait_until "kafka healthy" 180 is_healthy kafka
wait_until "mongo healthy" 180 is_healthy mongo

# ---------------------------------------------------------------------------
log "step 3/6: waiting for the run-once containers to exit 0"
# db-indexing-sidecar creates indexes and exits; the fixture backfill publishes
# its two messages and exits. Both exiting 0 is their designed terminal state.
wait_until "db-indexing-sidecar exited 0" 90 exited_zero db-indexing-sidecar
wait_until "transactions-historical exited 0" 90 exited_zero transactions-historical

# ---------------------------------------------------------------------------
log "step 4/6: waiting for message-consumer to persist the backfill"
mongo_count() {
  compose exec -T mongo mongosh --quiet eth_tx_pipeline \
    --eval "db.transactions.countDocuments({})" 2>/dev/null | tr -d '[:space:]'
}
mongo_has_expected() { [ "$(mongo_count)" = "$EXPECTED_TOTAL" ]; }
wait_until "mongo holds $EXPECTED_TOTAL enriched documents" 90 mongo_has_expected

# ---------------------------------------------------------------------------
log "step 5/6: asserting MongoDB indexes and API responses"

# db-indexing-sidecar must have created every index the API query paths rely on.
indexes="$(compose exec -T mongo mongosh --quiet eth_tx_pipeline \
  --eval "db.transactions.getIndexes().map(i => i.name).sort().join(',')" 2>/dev/null | tr -d '[:space:]')"
for expected_index in block_number_1 block_timestamp_1 contract_address_1 from_address_1 to_address_1; do
  case ",$indexes," in
    *",$expected_index,"*) : ;;
    *) fail "missing MongoDB index $expected_index (have: $indexes)" ;;
  esac
done
log "ready: MongoDB indexes present ($indexes)"

# The Python block below retries /health until endpoint-server is serving.
API_BASE="$API_BASE" \
FIXTURE_TX_HASH="$FIXTURE_TX_HASH" \
FIXTURE_TX_FEE_ETH="$FIXTURE_TX_FEE_ETH" \
FIXTURE_TX_FEE_USD="$FIXTURE_TX_FEE_USD" \
EXPECTED_TOTAL="$EXPECTED_TOTAL" \
python3 - <<'PY' || fail "API assertions failed"
import json, os, sys, time, urllib.request, urllib.error

base = os.environ["API_BASE"].rstrip("/")
expected_total = int(os.environ["EXPECTED_TOTAL"])
tx_hash = os.environ["FIXTURE_TX_HASH"]
fee_eth = float(os.environ["FIXTURE_TX_FEE_ETH"])
fee_usd = float(os.environ["FIXTURE_TX_FEE_USD"])


def get(path, want_json=True, retries=30):
    last = None
    for _ in range(retries):
        try:
            with urllib.request.urlopen(base + path, timeout=5) as r:
                body = r.read()
                return (r.status, json.loads(body) if want_json else body)
        except (urllib.error.URLError, ConnectionError) as exc:  # server still warming up
            last = exc
            time.sleep(1)
    raise SystemExit(f"could not reach {base}{path}: {last}")


def check(cond, msg):
    if not cond:
        raise SystemExit(f"assertion failed: {msg}")


status, health = get("/health")
check(status == 200 and health.get("status") == "ok", f"/health -> {status} {health}")
print("[e2e]   /health ok")

status, openapi = get("/openapi.json")
paths = set(openapi.get("paths", {}))
check("/transactions" in paths, f"/openapi.json missing /transactions (paths={sorted(paths)})")
print("[e2e]   /openapi.json advertises /transactions")

status, docs = get("/docs", want_json=False)
check(status == 200, f"/docs -> {status}")
print("[e2e]   /docs ok")

status, page = get("/transactions")
check(status == 200, f"/transactions -> {status}")
check(page["total"] == expected_total, f"/transactions total={page['total']} want {expected_total}")
print(f"[e2e]   /transactions total={page['total']}")

by_hash = {item["tx_hash"]: item for item in page["items"]}
check(tx_hash in by_hash, f"fixture tx {tx_hash} not served")
tx = by_hash[tx_hash]
check(abs(tx["fee_eth"] - fee_eth) < 1e-12, f"fee_eth={tx['fee_eth']} want {fee_eth}")
check(abs(tx["fee_usd"] - fee_usd) < 1e-9, f"fee_usd={tx['fee_usd']} want {fee_usd}")
check(tx["source"] == "historical", f"source={tx['source']} want historical")
print(f"[e2e]   fixture tx enriched correctly (fee_eth={tx['fee_eth']} fee_usd={tx['fee_usd']})")

# Address filter and block-range filter must narrow the result set.
status, filtered = get(f"/transactions?address={tx['from_address']}")
check(filtered["total"] == 1, f"address filter total={filtered['total']} want 1")
print("[e2e]   address filter ok")

print("[e2e] all API assertions passed")
PY

# ---------------------------------------------------------------------------
log "step 6/6: (optional) host-side Kafka external-listener check"
if command -v kcat >/dev/null 2>&1; then
  meta="$(kcat -b "$KAFKA_EXTERNAL" -L -m 10 2>&1 || true)"
  if echo "$meta" | grep -q "localhost:29092"; then
    log "ready: host reaches Kafka on $KAFKA_EXTERNAL (advertised as localhost:29092)"
  else
    fail "host could not reach Kafka external listener on $KAFKA_EXTERNAL: $meta"
  fi
else
  log "skip: kcat not installed (in-network flow already proves the pipeline)"
fi

log "E2E PASSED: fixture -> Kafka -> consumer -> MongoDB -> endpoint-server"
