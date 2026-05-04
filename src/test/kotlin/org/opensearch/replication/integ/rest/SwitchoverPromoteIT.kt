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
import org.opensearch.replication.startReplication
import org.opensearch.test.OpenSearchTestCase.assertBusy
import java.util.Locale
import java.util.concurrent.TimeUnit

/**
 * End-to-end verification of in-place promote: a follower index, after quiescence with a
 * fenced leader, is promoted via engine swap without re-bootstrap. The promoted index
 * accepts direct writes via the normal _doc API.
 *
 * This is the first test that exercises the actual in-place role swap described in
 * switchover-design.md (as opposed to the re-bootstrap workaround in
 * SwitchoverDirectionSwapIT).
 */
@ClusterConfigurations(
    ClusterConfiguration(clusterName = LEADER),
    ClusterConfiguration(clusterName = FOLLOWER)
)
class SwitchoverPromoteIT : MultiClusterRestTestCase() {

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

    fun `test promote enables direct writes on former follower without re-bootstrap`() {
        val leader = getClientForCluster(LEADER)
        val follower = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER)

        val indexName = "promote_" + randomAlphaOfLength(6).lowercase(Locale.ROOT)
        val shardCount = 3

        val createReq = CreateIndexRequest(indexName)
            .settings(Settings.builder().put("index.number_of_shards", shardCount).put("index.number_of_replicas", 0))
        assertThat(leader.indices().create(createReq, RequestOptions.DEFAULT).isAcknowledged).isTrue()

        follower.startReplication(
            StartReplicationRequest("source", indexName, indexName),
            waitForRestore = true
        )

        // Drive data on the leader.
        val docCount = 30
        for (i in 1..docCount) {
            performIndex(leader, indexName, i.toString(), """{"n": $i,"phase":"pre_promote"}""")
        }
        leader.lowLevelClient.performRequest(Request("POST", "/$indexName/_refresh"))

        // Confirm follower is receiving replication.
        assertBusy({
            follower.lowLevelClient.performRequest(Request("POST", "/$indexName/_refresh"))
            val count = countDocs(follower, indexName)
            assertThat(count).isEqualTo(docCount)
        }, 60L, TimeUnit.SECONDS)

        // Fence the leader index; new writes to the leader now rejected.
        installFence(leader, indexName)

        // Flush + get per-shard handoff checkpoints from the leader.
        val handoff = flushAndGetHandoffCheckpoint(leader, indexName)
        @Suppress("UNCHECKED_CAST")
        val handoffShards = handoff["shards"] as List<Map<String, Any>>
        val targets: Map<Int, Long> = handoffShards.associate {
            (it["shard"] as Int) to (it["handoff_seq_no"] as Number).toLong()
        }

        // Wait for the follower to catch up.
        verifyCaughtUp(follower, indexName, targets, timeoutMillis = 60_000)

        // Before promote: follower index is read-only (CCR write block). Confirm this by
        // expecting a failure on direct write to the follower.
        try {
            performIndex(follower, indexName, "pre_promote_direct", """{"should":"fail"}""")
            org.assertj.core.api.Assertions.fail<Unit>("expected direct write to follower to be rejected pre-promote")
        } catch (_: org.opensearch.client.ResponseException) {
            // expected — INDEX_REPLICATION_BLOCK is still in place
        }

        // Promote. This does the in-place engine swap on every primary shard and then
        // lifts INDEX_REPLICATION_BLOCK.
        val promote = promoteIndex(follower, indexName, targets)
        @Suppress("UNCHECKED_CAST")
        val promoteShards = promote["shards"] as List<Map<String, Any>>
        assertThat(promoteShards).hasSize(shardCount)
        for (shard in promoteShards) {
            val localCp = (shard["local_checkpoint"] as Number).toLong()
            val target = targets[shard["shard"] as Int]!!
            assertThat(localCp).isGreaterThanOrEqualTo(target)
        }

        // After promote: direct writes to the former follower must succeed.
        val postPromoteDocs = 10
        for (i in (docCount + 1)..(docCount + postPromoteDocs)) {
            val status = performIndex(follower, indexName, i.toString(), """{"n": $i,"phase":"post_promote"}""")
            assertThat(status).isIn(200, 201)
        }
        follower.lowLevelClient.performRequest(Request("POST", "/$indexName/_refresh"))

        // Total count on the former follower is pre + post.
        assertThat(countDocs(follower, indexName)).isEqualTo(docCount + postPromoteDocs)

        // The former leader remains fenced; it has no knowledge of the post-promote ops
        // (that reverse-replication is what future work on the demote side will handle).
        assertThat(countDocs(leader, indexName)).isEqualTo(docCount)
    }

    // ---- helpers ----

    private fun installFence(leader: RestHighLevelClient, indexName: String) {
        val resp = leader.lowLevelClient.performRequest(
            Request("POST", "/_plugins/_replication/switchover/$indexName/_fence")
        )
        fencedLeaderIndices.add(indexName)
        assertThat(resp.statusLine.statusCode).isEqualTo(200)
    }

    private fun flushAndGetHandoffCheckpoint(leader: RestHighLevelClient, indexName: String): Map<String, Any> {
        val resp = leader.lowLevelClient.performRequest(
            Request("POST", "/_plugins/_replication/switchover/$indexName/_flush_and_get_handoff_checkpoint")
        )
        assertThat(resp.statusLine.statusCode).isEqualTo(200)
        return parseJson(resp.entity.content.readAllBytes())
    }

    private fun verifyCaughtUp(
        follower: RestHighLevelClient,
        indexName: String,
        targets: Map<Int, Long>,
        timeoutMillis: Long
    ) {
        val req = Request("POST", "/_plugins/_replication/switchover/$indexName/_verify_caught_up")
        req.entity = StringEntity(
            """{"timeout_millis": $timeoutMillis, "target_seq_nos": {${targets.entries.joinToString(",") { """"${it.key}":${it.value}""" }}}}""",
            ContentType.APPLICATION_JSON
        )
        val resp = follower.lowLevelClient.performRequest(req)
        assertThat(resp.statusLine.statusCode).isEqualTo(200)
    }

    private fun promoteIndex(
        follower: RestHighLevelClient,
        indexName: String,
        targets: Map<Int, Long>
    ): Map<String, Any> {
        val req = Request("POST", "/_plugins/_replication/switchover/$indexName/_promote")
        req.entity = StringEntity(
            """{"target_seq_nos": {${targets.entries.joinToString(",") { """"${it.key}":${it.value}""" }}}}""",
            ContentType.APPLICATION_JSON
        )
        val resp = follower.lowLevelClient.performRequest(req)
        assertThat(resp.statusLine.statusCode).isEqualTo(200)
        return parseJson(resp.entity.content.readAllBytes())
    }

    private fun performIndex(client: RestHighLevelClient, indexName: String, id: String, body: String): Int {
        val req = Request("POST", "/$indexName/_doc/$id")
        req.entity = StringEntity(body, ContentType.APPLICATION_JSON)
        return client.lowLevelClient.performRequest(req).statusLine.statusCode
    }

    private fun countDocs(client: RestHighLevelClient, indexName: String): Int {
        client.lowLevelClient.performRequest(Request("POST", "/$indexName/_refresh"))
        val resp = client.lowLevelClient.performRequest(Request("GET", "/$indexName/_count"))
        return (parseJson(resp.entity.content.readAllBytes())["count"] as Number).toInt()
    }

    private fun parseJson(body: ByteArray): Map<String, Any> = XContentHelper.convertToMap(
        org.opensearch.core.common.bytes.BytesArray(body),
        false, XContentType.JSON
    ).v2()
}
