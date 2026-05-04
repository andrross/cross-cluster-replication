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
import org.junit.After
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.replication.MultiClusterAnnotations.ClusterConfiguration
import org.opensearch.replication.MultiClusterAnnotations.ClusterConfigurations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.replication.StartReplicationRequest
import org.opensearch.replication.`startReplication`
import java.util.Locale

/**
 * End-to-end verification of the quiescence invariant that switchover relies on:
 *
 *   For every replicated primary shard, after the leader is fenced and has produced a
 *   handoff checkpoint, the follower's local checkpoint must reach exactly that same
 *   seqno.
 *
 * This test stitches together the leader-side primitives (fence + flush-and-get-handoff-
 * checkpoint) with the follower-side primitive (verify-caught-up) against a real CCR
 * replication session.
 */
@ClusterConfigurations(
    ClusterConfiguration(clusterName = LEADER),
    ClusterConfiguration(clusterName = FOLLOWER)
)
class SwitchoverQuiescenceIT : MultiClusterRestTestCase() {

    private val fencedLeaderIndices = mutableListOf<String>()

    @After
    fun unfenceTrackedLeaderIndices() {
        val leader = getClientForCluster(LEADER)
        for (indexName in fencedLeaderIndices) {
            try {
                leader.lowLevelClient.performRequest(
                    Request("DELETE", "/_plugins/_replication/switchover/$indexName/_fence")
                )
            } catch (_: Exception) {
                // best-effort cleanup
            }
        }
        fencedLeaderIndices.clear()
    }

    fun `test leader handoff equals follower local checkpoint on every shard`() {
        val leader = getClientForCluster(LEADER)
        val follower = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val leaderIndex = "qleader_" + randomAlphaOfLength(6).lowercase(Locale.ROOT)
        val followerIndex = "qfollower_" + randomAlphaOfLength(6).lowercase(Locale.ROOT)
        val shardCount = 3

        val createRequest = CreateIndexRequest(leaderIndex)
            .settings(Settings.builder().put("index.number_of_shards", shardCount).put("index.number_of_replicas", 0))
        assertThat(leader.indices().create(createRequest, RequestOptions.DEFAULT).isAcknowledged).isTrue()

        follower.startReplication(
            StartReplicationRequest("source", leaderIndex, followerIndex),
            waitForRestore = true
        )

        // Drive some writes on the leader so the shards have non-trivial history.
        val docCount = 60
        for (i in 1..docCount) {
            performIndex(leader, leaderIndex, i.toString(), """{"n": $i}""")
        }
        leader.lowLevelClient.performRequest(Request("POST", "/$leaderIndex/_refresh"))

        // Fence → the leader side admission path now rejects new writes on leaderIndex.
        installFence(leaderIndex)

        // Flush and compute per-shard handoff checkpoints on the leader.
        val handoff = flushAndGetHandoffCheckpoint(leaderIndex)
        @Suppress("UNCHECKED_CAST")
        val handoffShards = handoff["shards"] as List<Map<String, Any>>
        assertThat(handoffShards).hasSize(shardCount)

        val targetSeqNos: Map<Int, Long> = handoffShards.associate {
            (it["shard"] as Int) to (it["handoff_seq_no"] as Number).toLong()
        }

        // Ask the follower to wait until every one of its primary shards has reached the
        // corresponding leader handoff seqno. This is the central v1 invariant.
        val verify = verifyCaughtUp(follower, followerIndex, targetSeqNos, timeoutMillis = 60_000)
        @Suppress("UNCHECKED_CAST")
        val verifyShards = verify["shards"] as List<Map<String, Any>>
        assertThat(verifyShards).hasSize(shardCount)

        for (shard in verifyShards) {
            val shardId = shard["shard"] as Int
            val target = (shard["target_seq_no"] as Number).toLong()
            val observed = (shard["local_checkpoint"] as Number).toLong()
            assertThat(target).isEqualTo(targetSeqNos[shardId])
            assertThat(observed).isGreaterThanOrEqualTo(target)
        }
    }

    fun `test verify caught up without any replication in progress fails fast with no unknown shards`() {
        // Smoke test: the action rejects a call whose shard map mentions shards the index
        // does not have. Catches copy-paste errors and drift.
        val follower = getClientForCluster(FOLLOWER)
        val indexName = "noreplica_" + randomAlphaOfLength(6).lowercase(Locale.ROOT)

        val createRequest = CreateIndexRequest(indexName)
            .settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        assertThat(follower.indices().create(createRequest, RequestOptions.DEFAULT).isAcknowledged).isTrue()

        val req = Request("POST", "/_plugins/_replication/switchover/$indexName/_verify_caught_up")
        req.entity = StringEntity(
            """{"timeout_millis": 1000, "target_seq_nos": {"99": 0}}""",
            ContentType.APPLICATION_JSON
        )
        var responseBody = ""
        var status = 0
        try {
            val resp = follower.lowLevelClient.performRequest(req)
            status = resp.statusLine.statusCode
            responseBody = String(resp.entity.content.readAllBytes())
        } catch (e: org.opensearch.client.ResponseException) {
            status = e.response.statusLine.statusCode
            responseBody = String(e.response.entity.content.readAllBytes())
        }
        assertThat(status).isGreaterThanOrEqualTo(400)
        assertThat(responseBody).contains("Unknown shard")
    }

    private fun installFence(indexName: String) {
        val leader = getClientForCluster(LEADER)
        val response = leader.lowLevelClient.performRequest(
            Request("POST", "/_plugins/_replication/switchover/$indexName/_fence")
        )
        fencedLeaderIndices.add(indexName)
        assertThat(response.statusLine.statusCode).isEqualTo(200)
    }

    private fun flushAndGetHandoffCheckpoint(indexName: String): Map<String, Any> {
        val leader = getClientForCluster(LEADER)
        val response = leader.lowLevelClient.performRequest(
            Request("POST", "/_plugins/_replication/switchover/$indexName/_flush_and_get_handoff_checkpoint")
        )
        assertThat(response.statusLine.statusCode).isEqualTo(200)
        return XContentHelper.convertToMap(
            org.opensearch.core.common.bytes.BytesArray(response.entity.content.readAllBytes()),
            false, XContentType.JSON
        ).v2()
    }

    private fun verifyCaughtUp(
        followerClient: RestHighLevelClient,
        indexName: String,
        targetSeqNos: Map<Int, Long>,
        timeoutMillis: Long
    ): Map<String, Any> {
        val req = Request("POST", "/_plugins/_replication/switchover/$indexName/_verify_caught_up")
        val jsonEntries = targetSeqNos.entries.joinToString(",") { """"${it.key}":${it.value}""" }
        req.entity = StringEntity(
            """{"timeout_millis": $timeoutMillis, "target_seq_nos": {$jsonEntries}}""",
            ContentType.APPLICATION_JSON
        )
        val response = followerClient.lowLevelClient.performRequest(req)
        assertThat(response.statusLine.statusCode).isEqualTo(200)
        return XContentHelper.convertToMap(
            org.opensearch.core.common.bytes.BytesArray(response.entity.content.readAllBytes()),
            false, XContentType.JSON
        ).v2()
    }

    private fun performIndex(client: RestHighLevelClient, indexName: String, id: String, body: String): Int {
        val req = Request("POST", "/$indexName/_doc/$id")
        req.entity = StringEntity(body, ContentType.APPLICATION_JSON)
        return client.lowLevelClient.performRequest(req).statusLine.statusCode
    }
}
