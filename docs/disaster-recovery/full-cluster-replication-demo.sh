#!/usr/bin/env bash
# Demonstrate full-cluster replication between two local OpenSearch clusters.
#
# Prerequisites: run from the repo root, with gradle able to build the plugin,
# plus jq + curl on PATH.
#
# Usage: docs/disaster-recovery/full-cluster-replication-demo.sh
#
# The script walks through:
#   1. Boot two clusters (leaderCluster + followCluster) via `./gradlew run`.
#   2. Configure replication: one remote connection on the follower pointing at
#      the leader, then put PRIMARY intent on the leader and SECONDARY intent
#      on the follower. Confirm GET returns the intent.
#   3. Create an index on the leader and write some documents. The follower's
#      long-poll should bootstrap the index and replay the ops.
#   4. Delete the replication arrangement by clearing the intent on both sides.
#      The follower-side index is preserved with all its data; the follower
#      marker is stripped so it becomes an ordinary writable index. We confirm
#      by indexing a fresh document into it on the former secondary.
#
# Between each phase the script pauses so you can inspect state from another
# terminal. Ctrl-C at any prompt aborts; the trap on EXIT tears the clusters
# down.
#
# Everything is curl against HTTP. No security enabled, no external dependencies
# beyond jq.

set -euo pipefail

# ----- config --------------------------------------------------------------

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD_DIR="$REPO_ROOT/build"
TESTCLUSTERS_DIR="$BUILD_DIR/testclusters"
INDEX_NAME="demo_index"
SHARDS=3
DOC_COUNT=20

# Customer-chosen identifier for this replication relationship. Both clusters store the
# intent under this ID. Stable for the life of the relationship.
RELATIONSHIP_ID="demo-rel"

# Cluster-settings alias configured on the follower pointing at the leader; also the
# follower intent's `remote_alias`.
REMOTE_ALIAS="leader"

# Identity labels the two clusters use to describe each other in the intent. On the
# PRIMARY (leader), `remote_alias` is cosmetic — the leader doesn't call back through it.
FOLLOWER_LOCAL_ALIAS="follower"

# ----- helpers -------------------------------------------------------------

log()  { printf '\n\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
warn() { printf '\n\033[1;33m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
die()  { printf '\n\033[1;31m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; exit 1; }
step() { printf '\n\033[1;32m==> %s\033[0m\n' "$*"; }

require() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

confirm_continue() {
  local prompt="${1:-Press Enter to continue (or Ctrl-C to abort)}"
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

doc_count() {
  local host="$1"
  curl -sS "http://$host/$INDEX_NAME/_count" 2>/dev/null | jq -r '.count // "n/a"'
}

index_exists() {
  local host="$1"
  curl -sS -o /dev/null -w '%{http_code}' "http://$host/$INDEX_NAME" 2>/dev/null | grep -q '^200$'
}

cluster_state_summary() {
  local host="$1"; local label="$2"
  local count="n/a"
  local has_index="no"
  if index_exists "$host"; then
    has_index="yes"
    count="$(doc_count "$host")"
  fi
  local intent
  intent="$(
    curl -sS -o /tmp/fcr_demo_intent -w '%{http_code}' \
      "http://$host/_replication/cluster/$RELATIONSHIP_ID" 2>/dev/null
  )"
  local intent_summary="(none)"
  if [[ "$intent" == "200" ]]; then
    intent_summary="$(jq -rc '{role,status,epoch,local_alias,remote_alias}' /tmp/fcr_demo_intent 2>/dev/null || cat /tmp/fcr_demo_intent)"
  fi
  printf '  [%s] index=%s docs=%s intent=%s\n' \
    "$label" "$has_index" "$count" "$intent_summary"
}

# ----- boot clusters -------------------------------------------------------

require jq
require curl

log "Starting two local OpenSearch clusters via ./gradlew run (this takes ~60s)"
log "Logs: $BUILD_DIR/gradlew-run.log"

rm -rf "$TESTCLUSTERS_DIR" 2>/dev/null || true

# -PnumNodes=1 keeps each cluster single-node — faster boot, avoids replica-init
# races the demo doesn't need. -Psecurity=false disables the security plugin.
./gradlew run -PnumNodes=1 -Psecurity=false \
  > "$BUILD_DIR/gradlew-run.log" 2>&1 &
GRADLE_PID=$!

cleanup() {
  log "Shutting down gradle (pid $GRADLE_PID)..."
  kill "$GRADLE_PID" 2>/dev/null || true
  wait "$GRADLE_PID" 2>/dev/null || true
}
trap cleanup EXIT

log "Waiting for leaderCluster http.ports to appear..."
LEADER_PORTS_FILE="$(wait_for_ports_file leaderCluster)"
# http.ports writes the node's bind address as "[::1]:<port>"; replace with
# localhost so copy-pasted curl commands work in any terminal.
LEADER_HOST="localhost:$(head -1 "$LEADER_PORTS_FILE" | sed 's/.*://')"
log "leaderCluster HTTP: $LEADER_HOST"

log "Waiting for followCluster http.ports to appear..."
FOLLOWER_PORTS_FILE="$(wait_for_ports_file followCluster)"
FOLLOWER_HOST="localhost:$(head -1 "$FOLLOWER_PORTS_FILE" | sed 's/.*://')"
log "followCluster HTTP: $FOLLOWER_HOST"

wait_for_http "$LEADER_HOST"
wait_for_http "$FOLLOWER_HOST"

LEADER="$LEADER_HOST"
FOLLOWER="$FOLLOWER_HOST"

step "Clusters up. Run these in another terminal to watch both sides:"
printf '\n  # Leader cluster\n'
printf "  watch -n1 \"curl -sS 'http://%s/_cat/indices/*,-.*?v' && echo && curl -sS 'http://%s/_replication/cluster/%s' | jq -C .\"\n" \
  "$LEADER" "$LEADER" "$RELATIONSHIP_ID"
printf '\n  # Follower cluster\n'
printf "  watch -n1 \"curl -sS 'http://%s/_cat/indices/*,-.*?v' && echo && curl -sS 'http://%s/_replication/cluster/%s' | jq -C . && echo && curl -sS 'http://%s/_replication/cluster/%s/status' | jq -C .\"\n\n" \
  "$FOLLOWER" "$FOLLOWER" "$RELATIONSHIP_ID" "$FOLLOWER" "$RELATIONSHIP_ID"

confirm_continue "Clusters ready. Press Enter to create the replication arrangement"

# ----- configure the replication arrangement -------------------------------

step "Wiring remote-cluster connection on the follower"

LEADER_TRANSPORT="localhost:$(head -1 "$TESTCLUSTERS_DIR/leaderCluster-0/logs/transport.ports" | sed 's/.*://')"
log "leader transport: $LEADER_TRANSPORT"

curl -sS -XPUT "http://$FOLLOWER/_cluster/settings" -H 'Content-Type: application/json' \
  -d "{\"persistent\":{\"cluster\":{\"remote\":{\"$REMOTE_ALIAS\":{\"seeds\":[\"$LEADER_TRANSPORT\"]}}}}}" | pretty

step "Installing SECONDARY intent on follower (leader PRIMARY is auto-installed)"
# One PUT against the secondary. The transport action hops to the peer via the
# cluster.remote.$REMOTE_ALIAS connection and installs the mirrored PRIMARY intent
# (with local_alias/remote_alias swapped) on the leader.
curl -sS -XPUT "http://$FOLLOWER/_replication/cluster/$RELATIONSHIP_ID" -H 'Content-Type: application/json' \
  -d "{\"role\":\"SECONDARY\",\"local_alias\":\"$FOLLOWER_LOCAL_ALIAS\",\"remote_alias\":\"$REMOTE_ALIAS\",\"epoch\":1,\"status\":\"STEADY\"}" | pretty

step "Confirm GET returns the intents"
log "leader intent:"
curl -sS "http://$LEADER/_replication/cluster/$RELATIONSHIP_ID" | pretty
log "follower intent:"
curl -sS "http://$FOLLOWER/_replication/cluster/$RELATIONSHIP_ID" | pretty

step "Follower's live status (long-poll loop should be active within a few seconds)"
for _ in $(seq 1 15); do
  body="$(curl -sS -o /dev/stdout -w '' "http://$FOLLOWER/_replication/cluster/$RELATIONSHIP_ID/status" 2>/dev/null)"
  if [[ -n "$body" ]]; then
    active="$(echo "$body" | jq -r '.loop_active // false')"
    if [[ "$active" == "true" ]]; then
      echo "$body" | pretty
      break
    fi
  fi
  sleep 1
done

confirm_continue "Replication arrangement is set up. Press Enter to create an index and write some documents"

# ----- create an index on the leader and write some documents --------------

step "Creating $INDEX_NAME on the leader ($SHARDS shards)"
curl -sS -XPUT "http://$LEADER/$INDEX_NAME" -H 'Content-Type: application/json' \
  -d "{\"settings\":{\"index.number_of_shards\":$SHARDS,\"index.number_of_replicas\":0}}" | pretty

step "Writing $DOC_COUNT documents to leader $INDEX_NAME"
for i in $(seq 1 "$DOC_COUNT"); do
  curl -sS -XPOST "http://$LEADER/$INDEX_NAME/_doc" -H 'Content-Type: application/json' \
    -d "{\"n\":$i,\"phase\":\"demo\"}" > /dev/null
done
curl -sS -XPOST "http://$LEADER/$INDEX_NAME/_refresh" > /dev/null

step "Waiting for follower to bootstrap the index and catch up"
# Bootstrap (snapshot restore) + ops replication. Usually seconds.
for _ in $(seq 1 60); do
  if index_exists "$FOLLOWER"; then
    curl -sS -XPOST "http://$FOLLOWER/$INDEX_NAME/_refresh" > /dev/null 2>&1 || true
    count="$(doc_count "$FOLLOWER")"
    if [[ "$count" == "$DOC_COUNT" ]]; then
      break
    fi
  fi
  sleep 1
done

cluster_state_summary "$LEADER" "leader"
cluster_state_summary "$FOLLOWER" "follower"

step "Follower status after catch-up"
curl -sS "http://$FOLLOWER/_replication/cluster/$RELATIONSHIP_ID/status" | pretty

confirm_continue "Index is replicated. Press Enter to tear down the replication arrangement"

# ----- tear down -----------------------------------------------------------

step "Clearing replication intent on both sides"
# DELETE on the secondary runs the sever synchronously before returning: close
# each follower index, strip REPLICATED_INDEX_SETTING so the engine factory
# picks InternalEngine (writable) on reopen, then reopen. Data and settings
# are preserved; only the follower marker is gone.
curl -sS -XDELETE "http://$FOLLOWER/_replication/cluster/$RELATIONSHIP_ID" | pretty
curl -sS -XDELETE "http://$LEADER/_replication/cluster/$RELATIONSHIP_ID"   | pretty

cluster_state_summary "$LEADER" "leader"
cluster_state_summary "$FOLLOWER" "follower"

step "Both intents should now be absent (404)"
log "leader:"
curl -sS -o /dev/stdout -w '\n  http=%{http_code}\n' "http://$LEADER/_replication/cluster/$RELATIONSHIP_ID" || true
log "follower:"
curl -sS -o /dev/stdout -w '\n  http=%{http_code}\n' "http://$FOLLOWER/_replication/cluster/$RELATIONSHIP_ID" || true

step "Follower's $INDEX_NAME should still be present with all $DOC_COUNT docs"
if index_exists "$FOLLOWER"; then
  post_count="$(doc_count "$FOLLOWER")"
  log "follower doc count after sever: $post_count (expected $DOC_COUNT)"
else
  warn "follower dropped $INDEX_NAME unexpectedly — sever should have preserved it"
fi

step "Write a fresh document on the former secondary to prove it's writable"
curl -sS -XPOST "http://$FOLLOWER/$INDEX_NAME/_doc" -H 'Content-Type: application/json' \
  -d '{"n":"post-sever","phase":"independent"}' | pretty
curl -sS -XPOST "http://$FOLLOWER/$INDEX_NAME/_refresh" > /dev/null
log "follower doc count after write: $(doc_count "$FOLLOWER") (expected $((DOC_COUNT + 1)))"

confirm_continue "Done. Press Enter to shut down the clusters"
