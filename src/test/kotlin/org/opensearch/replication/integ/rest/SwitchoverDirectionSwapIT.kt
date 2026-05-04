/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.replication.integ.rest

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.fail
import org.junit.After
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.replication.MultiClusterAnnotations.ClusterConfiguration
import org.opensearch.replication.MultiClusterAnnotations.ClusterConfigurations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.startReplication
import org.opensearch.test.OpenSearchTestCase.assertBusy
import java.util.Locale
import java.util.concurrent.TimeUnit

/**
 * In-place direction flip with reverse replication — full round trip including writes in
 * both directions.
 *
 * The flip procedure uses only in-place primitives:
 *   1. Fence the current leader (cluster-state write block).
 *   2. Flush + get per-shard handoff seqnos.
 *   3. Verify the follower has caught up.
 *   4. Promote the follower (engine swapped in place; CCR block lifted).
 *   5. Demote the former leader (close/reopen with follower settings).
 *   6. Start replication in the reverse direction with skip_bootstrap=true — the demoted
 *      index already has the data, so no snapshot restore runs.
 *
 * Timeline this test validates:
 *   - A is leader, B is follower. Write on A, replicates to B.
 *   - Flip → B is leader, A is follower. Write on B, replicates to A.
 *   - Flip → A is leader, B is follower. Write on A, replicates to B.
 *
 * Both flips are in-place (no re-bootstrap), and writes propagate in both directions.
 */
@ClusterConfigurations(
    ClusterConfiguration(clusterName = LEADER),
    ClusterConfiguration(clusterName = FOLLOWER)
)
class SwitchoverDirectionSwapIT : MultiClusterRestTestCase() {

    companion object {
        private const val CONN_A_TO_B = "a_to_b"   // alias on B pointing to A (B pulls from A)
        private const val CONN_B_TO_A = "b_to_a"   // alias on A pointing to B (A pulls from B)
    }

    private val replicationLeftBehind = mutableListOf<Pair<String, String>>() // (cluster, index)

    @After
    fun preCleanReplicationLeftovers() {
        for ((cluster, indexName) in replicationLeftBehind) {
            val client = getClientForCluster(cluster)
            try {
                val stopReq = Request("POST", "/_plugins/_replication/$indexName/_stop")
                stopReq.setJsonEntity("{}")
                client.lowLevelClient.performRequest(stopReq)
            } catch (_: Exception) { /* best-effort */ }
            try {
                client.lowLevelClient.performRequest(Request("DELETE", "/$indexName"))
            } catch (_: Exception) { /* best-effort */ }
        }
        replicationLeftBehind.clear()
    }

    fun `test in place direction flip with reverse replication round trip`() {
        val a = getClientForCluster(LEADER)
        val b = getClientForCluster(FOLLOWER)

        val indexName = "swap_" + randomAlphaOfLength(6).lowercase(Locale.ROOT)
        val shardCount = 3

        // Both directions' remote-cluster aliases, set up once.
        // CONN_A_TO_B lives on B and points at A (B pulls from A).
        // CONN_B_TO_A lives on A and points at B (A pulls from B).
        createConnectionBetweenClusters(FOLLOWER, LEADER, CONN_A_TO_B)
        createConnectionBetweenClusters(LEADER, FOLLOWER, CONN_B_TO_A)

        replicationLeftBehind.add(LEADER to indexName)
        replicationLeftBehind.add(FOLLOWER to indexName)

        // ===== Phase 1: A is leader, B is follower =====
        createIndex(a, indexName, shardCount)
        b.startReplication(StartReplicationRequest(CONN_A_TO_B, indexName, indexName), waitForRestore = true)

        val phase1Docs = 20
        indexDocs(a, indexName, 1..phase1Docs)
        assertDocCount(a, indexName, phase1Docs)
        assertBusy({
            b.lowLevelClient.performRequest(Request("POST", "/$indexName/_refresh"))
            assertThat(countDocs(b, indexName)).isEqualTo(phase1Docs)
        }, 60L, TimeUnit.SECONDS)
        assertWriteRejected(b, indexName)

        // ===== Flip #1: A → B becomes B → A =====
        inPlaceFlip(currentLeader = a, currentFollower = b, indexName = indexName, shardCount = shardCount)
        // Start reverse replication from B into A. No re-bootstrap — A already has data.
        startReplicationAfterDemote(followerClient = a, connectionName = CONN_B_TO_A, indexName = indexName)

        // ===== Phase 2: B is leader, A is follower =====
        // Direct writes to A are rejected (CCR block back in place).
        assertWriteRejected(a, indexName)
        // New writes on B propagate to A via the reverse pull.
        val phase2Docs = 15
        indexDocs(b, indexName, (phase1Docs + 1)..(phase1Docs + phase2Docs))
        val totalAfterPhase2 = phase1Docs + phase2Docs
        assertDocCount(b, indexName, totalAfterPhase2)
        assertBusy({
            a.lowLevelClient.performRequest(Request("POST", "/$indexName/_refresh"))
            assertThat(countDocs(a, indexName)).isEqualTo(totalAfterPhase2)
        }, 60L, TimeUnit.SECONDS)

        // ===== Flip #2: B → A becomes A → B =====
        inPlaceFlip(currentLeader = b, currentFollower = a, indexName = indexName, shardCount = shardCount)
        // Start forward replication again from A into B. Again no re-bootstrap.
        startReplicationAfterDemote(followerClient = b, connectionName = CONN_A_TO_B, indexName = indexName)

        // ===== Phase 3: A is leader again, B is follower again =====
        assertWriteRejected(b, indexName)
        val phase3Docs = 10
        indexDocs(a, indexName, (totalAfterPhase2 + 1)..(totalAfterPhase2 + phase3Docs))
        val totalAfterPhase3 = totalAfterPhase2 + phase3Docs
        assertDocCount(a, indexName, totalAfterPhase3)
        assertBusy({
            b.lowLevelClient.performRequest(Request("POST", "/$indexName/_refresh"))
            assertThat(countDocs(b, indexName)).isEqualTo(totalAfterPhase3)
        }, 60L, TimeUnit.SECONDS)
    }

    /**
     * In-place flip for [indexName]. Precondition: [currentLeader] is accepting writes,
     * [currentFollower] is actively replicating from it.
     *
     * Postcondition: [currentFollower] accepts writes; [currentLeader] is in follower
     * shape (has REPLICATED_INDEX_SETTING + CCR write block). Reverse replication is NOT
     * started by this method — call startReplicationAfterDemote separately.
     */
    private fun inPlaceFlip(
        currentLeader: RestHighLevelClient,
        currentFollower: RestHighLevelClient,
        indexName: String,
        shardCount: Int
    ) {
        // 1. Fence.
        val fenceResp = currentLeader.lowLevelClient.performRequest(
            Request("POST", "/_plugins/_replication/switchover/$indexName/_fence")
        )
        assertThat(fenceResp.statusLine.statusCode).isEqualTo(200)

        // 2. Flush + get handoff checkpoints.
        val handoffResp = currentLeader.lowLevelClient.performRequest(
            Request("POST", "/_plugins/_replication/switchover/$indexName/_flush_and_get_handoff_checkpoint")
        )
        assertThat(handoffResp.statusLine.statusCode).isEqualTo(200)
        @Suppress("UNCHECKED_CAST")
        val handoffShards = parseJson(handoffResp.entity.content.readAllBytes())["shards"] as List<Map<String, Any>>
        assertThat(handoffShards).hasSize(shardCount)
        val targets: Map<Int, Long> = handoffShards.associate {
            (it["shard"] as Int) to (it["handoff_seq_no"] as Number).toLong()
        }

        // 3. Verify follower caught up.
        val verifyReq = Request("POST", "/_plugins/_replication/switchover/$indexName/_verify_caught_up")
        verifyReq.entity = StringEntity(
            """{"timeout_millis": 60000, "target_seq_nos": {${targets.entries.joinToString(",") { """"${it.key}":${it.value}""" }}}}""",
            ContentType.APPLICATION_JSON
        )
        val verifyResp = currentFollower.lowLevelClient.performRequest(verifyReq)
        assertThat(verifyResp.statusLine.statusCode).isEqualTo(200)

        // 4. Promote follower.
        val promoteReq = Request("POST", "/_plugins/_replication/switchover/$indexName/_promote")
        promoteReq.entity = StringEntity(
            """{"target_seq_nos": {${targets.entries.joinToString(",") { """"${it.key}":${it.value}""" }}}}""",
            ContentType.APPLICATION_JSON
        )
        val promoteResp = currentFollower.lowLevelClient.performRequest(promoteReq)
        assertThat(promoteResp.statusLine.statusCode).isEqualTo(200)

        // 5. Demote former leader.
        val demoteReq = Request("POST", "/_plugins/_replication/switchover/$indexName/_demote")
        demoteReq.entity = StringEntity("""{"leader_index":"$indexName"}""", ContentType.APPLICATION_JSON)
        val demoteResp = currentLeader.lowLevelClient.performRequest(demoteReq)
        assertThat(demoteResp.statusLine.statusCode).isEqualTo(200)
    }

    /**
     * Start replication. The server detects that the follower index already exists in
     * follower shape (from a prior demote) and skips the snapshot bootstrap
     * automatically.
     */
    private fun startReplicationAfterDemote(
        followerClient: RestHighLevelClient,
        connectionName: String,
        indexName: String
    ) {
        val req = Request("PUT", "/_plugins/_replication/$indexName/_start?wait_for_restore=true")
        req.setJsonEntity(
            """{
              "leader_alias": "$connectionName",
              "leader_index": "$indexName",
              "use_roles": {
                "leader_cluster_role": "leader_role",
                "follower_cluster_role": "follower_role"
              }
            }"""
        )
        val resp = followerClient.lowLevelClient.performRequest(req)
        assertThat(resp.statusLine.statusCode).isEqualTo(200)
    }

    // ---- helpers ----

    private fun createIndex(client: RestHighLevelClient, indexName: String, shardCount: Int) {
        val req = CreateIndexRequest(indexName)
            .settings(Settings.builder().put("index.number_of_shards", shardCount).put("index.number_of_replicas", 0))
        assertThat(client.indices().create(req, RequestOptions.DEFAULT).isAcknowledged).isTrue()
    }

    private fun indexDocs(client: RestHighLevelClient, indexName: String, ids: IntRange) {
        for (i in ids) {
            val req = Request("POST", "/$indexName/_doc/$i")
            req.entity = StringEntity("""{"n":$i}""", ContentType.APPLICATION_JSON)
            val resp = client.lowLevelClient.performRequest(req)
            assertThat(resp.statusLine.statusCode).isIn(200, 201)
        }
        client.lowLevelClient.performRequest(Request("POST", "/$indexName/_refresh"))
    }

    private fun countDocs(client: RestHighLevelClient, indexName: String): Int {
        client.lowLevelClient.performRequest(Request("POST", "/$indexName/_refresh"))
        val resp = client.lowLevelClient.performRequest(Request("GET", "/$indexName/_count"))
        return (parseJson(resp.entity.content.readAllBytes())["count"] as Number).toInt()
    }

    private fun assertDocCount(client: RestHighLevelClient, indexName: String, expected: Int) {
        assertThat(countDocs(client, indexName)).isEqualTo(expected)
    }

    private fun assertWriteRejected(client: RestHighLevelClient, indexName: String) {
        val req = Request("POST", "/$indexName/_doc/probe_reject_${randomAlphaOfLength(4).lowercase(Locale.ROOT)}")
        req.entity = StringEntity("""{"probe":"rejected"}""", ContentType.APPLICATION_JSON)
        try {
            client.lowLevelClient.performRequest(req)
            fail<Unit>("expected write to $indexName to be rejected")
        } catch (e: ResponseException) {
            assertThat(e.response.statusLine.statusCode).isEqualTo(403)
        }
    }

    private fun parseJson(body: ByteArray): Map<String, Any> = XContentHelper.convertToMap(
        org.opensearch.core.common.bytes.BytesArray(body),
        false, XContentType.JSON
    ).v2()
}
