#!/usr/bin/env bash
# Demonstrate in-place switchover between two local OpenSearch clusters.
#
# Prerequisites: run from the repo root, with gradle able to build the plugin.
#
# Usage: docs/disaster-recovery/switchover-demo.sh
#
# The script:
#   1. Boots two clusters (leaderCluster + followCluster) via `./gradlew run` in
#      the background, single node per cluster.
#   2. Discovers each cluster's HTTP port from the test-cluster .ports files.
#   3. Configures replication: remote connections in both directions, starts A→B replication.
#   4. Runs two in-place direction flips (A→B → B→A → A→B), showing the state of
#      the fence block, REPLICATED_INDEX_SETTING, and doc counts at each step.
#   5. Tears the clusters down on exit.
#
# Everything is curl against HTTP. No security enabled. No external dependencies
# beyond jq.

set -euo pipefail

# ----- config --------------------------------------------------------------

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD_DIR="$REPO_ROOT/build"
TESTCLUSTERS_DIR="$BUILD_DIR/testclusters"
INDEX_NAME="demo_index"
SHARDS=3

# Connection aliases (these are remote-cluster settings that live on the *caller*
# side of the connection and point at the remote cluster):
#   a_to_b lives on B and points at A  — used when B pulls from A
#   b_to_a lives on A and points at B  — used when A pulls from B
CONN_A_TO_B="a_to_b"
CONN_B_TO_A="b_to_a"

# ----- helpers -------------------------------------------------------------

log()  { printf '\n\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
warn() { printf '\n\033[1;33m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
die()  { printf '\n\033[1;31m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; exit 1; }
step() { printf '\n\033[1;32m==> %s\033[0m\n' "$*"; }

require() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

confirm_continue() {
  local prompt="${1:-Press Enter to continue with the switchover (or Ctrl-C to abort)}"
  # Read from the controlling terminal explicitly so this still works if the
  # script's stdin is redirected elsewhere.
  printf '\n\033[1;36m>>> %s: \033[0m' "$prompt"
  read -r _ < /dev/tty
}

wait_for_ports_file() {
  local cluster="$1"
  local timeout=180
  local elapsed=0
  local ports_file
  while [[ $elapsed -lt $timeout ]]; do
    ports_file="$(find "$TESTCLUSTERS_DIR/${cluster}-0/logs" -maxdepth 1 -name 'http.ports' 2>/dev/null | head -1)"
    if [[ -n "$ports_file" && -s "$ports_file" ]]; then
      echo "$ports_file"
      return 0
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  die "timed out waiting for $cluster http.ports file"
}

wait_for_http() {
  local host="$1"
  local timeout=120
  local elapsed=0
  while [[ $elapsed -lt $timeout ]]; do
    if curl -sS -o /dev/null -w '%{http_code}' "http://$host/_cluster/health" 2>/dev/null | grep -q '^200$'; then
      return 0
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  die "timed out waiting for HTTP on $host"
}

pretty() { jq -C '.' 2>/dev/null || cat; }

cluster_state_summary() {
  local host="$1"; local label="$2"
  local count; count="$(curl -sS "http://$host/$INDEX_NAME/_count" 2>/dev/null | jq -r '.count // "n/a"')"
  local has_replicated; has_replicated="$(
    curl -sS "http://$host/$INDEX_NAME/_settings" \
      | jq -r --arg idx "$INDEX_NAME" '.[$idx].settings.index.plugins.replication.follower.leader_index // "none"'
  )"
  local block_ids; block_ids="$(
    curl -sS "http://$host/_cluster/state/blocks/$INDEX_NAME" \
      | jq -r --arg idx "$INDEX_NAME" '(.blocks.indices[$idx] // {}) | keys | join(",")'
  )"
  printf '  [%s] docs=%s  replicated_index_setting=%s  blocks=[%s]\n' \
    "$label" "$count" "$has_replicated" "$block_ids"
}

# ----- boot clusters -------------------------------------------------------

require jq
require curl

log "Starting two local OpenSearch clusters via ./gradlew run (this takes ~60s)"
log "Logs: $BUILD_DIR/gradlew-run.log"

rm -rf "$TESTCLUSTERS_DIR" 2>/dev/null || true

# -PnumNodes=1 keeps each cluster single-node — faster boot, avoids replica-init
# races the demo doesn't need. -Psecurity=false disables the security plugin (so
# curl without auth works).
./gradlew run -PnumNodes=1 -Psecurity=false \
  > "$BUILD_DIR/gradlew-run.log" 2>&1 &
GRADLE_PID=$!

cleanup() {
  log "Shutting down gradle (pid $GRADLE_PID)..."
  # gradle's stop-on-signal needs a bit of patience
  kill "$GRADLE_PID" 2>/dev/null || true
  wait "$GRADLE_PID" 2>/dev/null || true
}
trap cleanup EXIT

log "Waiting for leaderCluster http.ports to appear..."
LEADER_PORTS_FILE="$(wait_for_ports_file leaderCluster)"
LEADER_HOST="$(head -1 "$LEADER_PORTS_FILE")"
log "leaderCluster HTTP: $LEADER_HOST"

log "Waiting for followCluster http.ports to appear..."
FOLLOWER_PORTS_FILE="$(wait_for_ports_file followCluster)"
FOLLOWER_HOST="$(head -1 "$FOLLOWER_PORTS_FILE")"
log "followCluster HTTP: $FOLLOWER_HOST"

wait_for_http "$LEADER_HOST"
wait_for_http "$FOLLOWER_HOST"

# For the demo, treat leaderCluster as "A" and followCluster as "B".
A="$LEADER_HOST"
B="$FOLLOWER_HOST"

step "Clusters up. Run these in another terminal to watch both sides:"
JQ_FILTER='.hits.hits[] | "_id: \(._id), \(._source | tostring)"'
printf '\n  # Cluster A (leaderCluster)\n'
printf "  curl -sS 'http://%s/%s/_search' -H 'Content-Type: application/json' -d '%s' | jq -r '%s'\n" \
  "$A" "$INDEX_NAME" '{"query":{"match_all":{}},"size":100,"sort":[{"n":"asc"}]}' "$JQ_FILTER"
printf '\n  # Cluster B (followCluster)\n'
printf "  curl -sS 'http://%s/%s/_search' -H 'Content-Type: application/json' -d '%s' | jq -r '%s'\n\n" \
  "$B" "$INDEX_NAME" '{"query":{"match_all":{}},"size":100,"sort":[{"n":"asc"}]}' "$JQ_FILTER"

# ----- extract transport ports (for remote-cluster seeds) ------------------

# The remote connection seeds take host:transport-port. Transport ports are in
# the transport.ports file, same directory.
A_TRANSPORT="$(head -1 "$TESTCLUSTERS_DIR/leaderCluster-0/logs/transport.ports")"
B_TRANSPORT="$(head -1 "$TESTCLUSTERS_DIR/followCluster-0/logs/transport.ports")"
log "A transport: $A_TRANSPORT  |  B transport: $B_TRANSPORT"

# ----- configure remote connections ---------------------------------------

step "Configuring remote-cluster aliases in both directions"

# $CONN_A_TO_B lives on B, points at A.
curl -sS -XPUT "http://$B/_cluster/settings" -H 'Content-Type: application/json' \
  -d "{\"persistent\":{\"cluster\":{\"remote\":{\"$CONN_A_TO_B\":{\"seeds\":[\"$A_TRANSPORT\"]}}}}}" | pretty

# $CONN_B_TO_A lives on A, points at B.
curl -sS -XPUT "http://$A/_cluster/settings" -H 'Content-Type: application/json' \
  -d "{\"persistent\":{\"cluster\":{\"remote\":{\"$CONN_B_TO_A\":{\"seeds\":[\"$B_TRANSPORT\"]}}}}}" | pretty

# ----- create index on A and start replication to B -----------------------

step "Creating index $INDEX_NAME on A ($SHARDS shards)"
curl -sS -XPUT "http://$A/$INDEX_NAME" -H 'Content-Type: application/json' \
  -d "{\"settings\":{\"index.number_of_shards\":$SHARDS,\"index.number_of_replicas\":0}}" | pretty

step "Starting replication A→B via the existing plugin endpoint"
curl -sS -XPUT "http://$B/_plugins/_replication/$INDEX_NAME/_start?wait_for_restore=true" \
  -H 'Content-Type: application/json' \
  -d "{\"leader_alias\":\"$CONN_A_TO_B\",\"leader_index\":\"$INDEX_NAME\",\"use_roles\":{\"leader_cluster_role\":\"leader_role\",\"follower_cluster_role\":\"follower_role\"}}" \
  | pretty

# ----- demo phase 1: write on A, observe on B -----------------------------

step "Phase 1: A is leader. Indexing 20 docs on A."
for i in $(seq 1 20); do
  curl -sS -XPOST "http://$A/$INDEX_NAME/_doc" -H 'Content-Type: application/json' \
    -d "{\"n\":$i,\"phase\":\"initial\"}" > /dev/null
done
curl -sS -XPOST "http://$A/$INDEX_NAME/_refresh" > /dev/null

log "Waiting for B to catch up..."
for _ in $(seq 1 30); do
  count_b="$(curl -sS "http://$B/$INDEX_NAME/_count" | jq -r '.count')"
  if [[ "$count_b" == "20" ]]; then break; fi
  sleep 1
done
curl -sS -XPOST "http://$B/$INDEX_NAME/_refresh" > /dev/null

cluster_state_summary "$A" "A"
cluster_state_summary "$B" "B"

confirm_continue "Ready to flip direction A→B ⇒ B→A"

# ----- flip #1: A→B becomes B→A -------------------------------------------

flip() {
  local current_leader="$1"; local current_follower="$2"
  local leader_label="$3"; local follower_label="$4"; local reverse_conn="$5"

  step "Flip: $leader_label (leader) → $follower_label (leader)"

  log "[$leader_label] Fence writes"
  curl -sS -XPOST "http://$current_leader/_plugins/_replication/switchover/$INDEX_NAME/_fence" | pretty

  log "[$leader_label] Flush and get handoff checkpoint"
  local handoff
  handoff="$(curl -sS -XPOST "http://$current_leader/_plugins/_replication/switchover/$INDEX_NAME/_flush_and_get_handoff_checkpoint")"
  echo "$handoff" | pretty

  # Build per-shard target map for verify-caught-up and promote.
  local targets_json
  targets_json="$(echo "$handoff" | jq -c '[.shards[] | {(.shard|tostring): .handoff_seq_no}] | add')"

  log "[$follower_label] Verify caught up to: $targets_json"
  curl -sS -XPOST "http://$current_follower/_plugins/_replication/switchover/$INDEX_NAME/_verify_caught_up" \
    -H 'Content-Type: application/json' \
    -d "{\"timeout_millis\":60000,\"target_seq_nos\":$targets_json}" | pretty

  log "[$follower_label] Promote (in-place engine swap)"
  curl -sS -XPOST "http://$current_follower/_plugins/_replication/switchover/$INDEX_NAME/_promote" \
    -H 'Content-Type: application/json' \
    -d "{\"target_seq_nos\":$targets_json}" | pretty

  log "[$leader_label] Demote (close/reopen with follower settings)"
  curl -sS -XPOST "http://$current_leader/_plugins/_replication/switchover/$INDEX_NAME/_demote" \
    -H 'Content-Type: application/json' \
    -d "{\"leader_index\":\"$INDEX_NAME\"}" | pretty

  log "[$leader_label] Start reverse replication (server auto-detects post-demote state, skips bootstrap)"
  curl -sS -XPUT "http://$current_leader/_plugins/_replication/$INDEX_NAME/_start?wait_for_restore=true" \
    -H 'Content-Type: application/json' \
    -d "{\"leader_alias\":\"$reverse_conn\",\"leader_index\":\"$INDEX_NAME\",\"use_roles\":{\"leader_cluster_role\":\"leader_role\",\"follower_cluster_role\":\"follower_role\"}}" | pretty

  cluster_state_summary "$current_leader" "$leader_label"
  cluster_state_summary "$current_follower" "$follower_label"
}

flip "$A" "$B" "A" "B" "$CONN_B_TO_A"

# ----- demo phase 2: B is leader, write on B, observe on A ----------------

step "Phase 2: B is leader. Indexing 10 more docs on B."
for i in $(seq 21 30); do
  curl -sS -XPOST "http://$B/$INDEX_NAME/_doc" -H 'Content-Type: application/json' \
    -d "{\"n\":$i,\"phase\":\"after_flip_1\"}" > /dev/null
done
curl -sS -XPOST "http://$B/$INDEX_NAME/_refresh" > /dev/null

log "Waiting for A to catch up..."
for _ in $(seq 1 30); do
  count_a="$(curl -sS "http://$A/$INDEX_NAME/_count" | jq -r '.count')"
  if [[ "$count_a" == "30" ]]; then break; fi
  sleep 1
done
curl -sS -XPOST "http://$A/$INDEX_NAME/_refresh" > /dev/null

cluster_state_summary "$A" "A"
cluster_state_summary "$B" "B"

# ----- flip #2: B→A becomes A→B -------------------------------------------

flip "$B" "$A" "B" "A" "$CONN_A_TO_B"

# ----- demo phase 3: A is leader again, write on A, observe on B ----------

step "Phase 3: A is leader again. Indexing 5 more docs on A."
for i in $(seq 31 35); do
  curl -sS -XPOST "http://$A/$INDEX_NAME/_doc" -H 'Content-Type: application/json' \
    -d "{\"n\":$i,\"phase\":\"after_flip_2\"}" > /dev/null
done
curl -sS -XPOST "http://$A/$INDEX_NAME/_refresh" > /dev/null

log "Waiting for B to catch up..."
for _ in $(seq 1 30); do
  count_b="$(curl -sS "http://$B/$INDEX_NAME/_count" | jq -r '.count')"
  if [[ "$count_b" == "35" ]]; then break; fi
  sleep 1
done
curl -sS -XPOST "http://$B/$INDEX_NAME/_refresh" > /dev/null

cluster_state_summary "$A" "A"
cluster_state_summary "$B" "B"

step "Done. Two in-place direction flips + reverse replication validated."
echo "Clusters will be torn down when you exit."
echo "Press Ctrl-C to exit, or leave running to inspect further."

# Keep the script alive so the clusters don't get torn down until the user is ready.
wait "$GRADLE_PID"
