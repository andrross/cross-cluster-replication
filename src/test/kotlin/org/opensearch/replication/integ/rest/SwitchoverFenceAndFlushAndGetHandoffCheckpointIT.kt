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
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.common.settings.Settings
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.replication.MultiClusterAnnotations.ClusterConfiguration
import org.opensearch.replication.MultiClusterAnnotations.ClusterConfigurations
import org.opensearch.replication.MultiClusterRestTestCase
import java.util.Locale

@ClusterConfigurations(
    ClusterConfiguration(clusterName = LEADER)
)
class SwitchoverFenceAndFlushAndGetHandoffCheckpointIT : MultiClusterRestTestCase() {

    private val fencedIndices = mutableListOf<String>()

    // Parent's wipeCluster runs @After as well. JUnit 4 runs subclass @After methods before
    // superclass @After methods, so unfencing here happens before wipeIndicesFromCluster is
    // invoked by the parent. Any fenced index the test left behind would otherwise refuse
    // deletion because the fence block carries METADATA_WRITE.
    @After
    fun unfenceAllTrackedIndices() {
        val leader = getClientForCluster(LEADER)
        for (indexName in fencedIndices) {
            try {
                leader.lowLevelClient.performRequest(
                    Request("DELETE", "/_plugins/_replication/switchover/$indexName/_fence")
                )
            } catch (_: Exception) {
                // best-effort cleanup
            }
        }
        fencedIndices.clear()
    }

    fun `test fence blocks writes and single-shard handoff checkpoint is sane`() {
        val leader = getClientForCluster(LEADER)
        val indexName = "handoff_single_" + randomAlphaOfLength(6).lowercase(Locale.ROOT)

        val createRequest = CreateIndexRequest(indexName)
            .settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        assertThat(leader.indices().create(createRequest, RequestOptions.DEFAULT).isAcknowledged).isTrue()

        val docCount = 10
        for (i in 1..docCount) {
            performIndex(indexName, i.toString(), """{"n": $i}""")
        }
        leader.lowLevelClient.performRequest(Request("POST", "/$indexName/_refresh"))

        assertThat(performIndex(indexName, "pre_fence", """{"status":"ok"}""")).isIn(200, 201)

        installFence(indexName)

        // Writes must be refused by the fence block.
        try {
            performIndex(indexName, "post_fence", """{"status":"should fail"}""")
            fail<Unit>("Expected indexing to be blocked after fencing")
        } catch (e: ResponseException) {
            assertThat(e.response.statusLine.statusCode).isEqualTo(403)
        }

        val result = flushAndGetHandoffCheckpoint(indexName)
        assertThat(result["index"]).isEqualTo(indexName)
        assertThat((result["shard_count"] as Number).toInt()).isEqualTo(1)

        @Suppress("UNCHECKED_CAST")
        val shards = result["shards"] as List<Map<String, Any>>
        val shard0 = shards.single()
        assertThat(shard0["shard"]).isEqualTo(0)
        assertThat(shard0["primary"]).isEqualTo(true)

        // docCount + 1 pre_fence doc = docCount + 1 ops, seqnos 0-based → max seq = docCount.
        val maxSeq = (shard0["max_seq_no"] as Number).toLong()
        val handoff = (shard0["handoff_seq_no"] as Number).toLong()
        assertThat(maxSeq).isEqualTo(docCount.toLong())
        assertThat(handoff).isEqualTo(maxSeq)

        // Idempotent: re-running returns the same seqnos.
        val result2 = flushAndGetHandoffCheckpoint(indexName)
        @Suppress("UNCHECKED_CAST")
        val shard0b = (result2["shards"] as List<Map<String, Any>>).single()
        assertThat((shard0b["handoff_seq_no"] as Number).toLong()).isEqualTo(handoff)
    }

    fun `test multi-shard handoff checkpoint covers every primary`() {
        val leader = getClientForCluster(LEADER)
        val indexName = "handoff_multi_" + randomAlphaOfLength(6).lowercase(Locale.ROOT)

        val shardCount = 3
        val createRequest = CreateIndexRequest(indexName)
            .settings(Settings.builder().put("index.number_of_shards", shardCount).put("index.number_of_replicas", 0))
        assertThat(leader.indices().create(createRequest, RequestOptions.DEFAULT).isAcknowledged).isTrue()

        val docCount = 60
        for (i in 1..docCount) {
            performIndex(indexName, i.toString(), """{"n": $i}""")
        }
        leader.lowLevelClient.performRequest(Request("POST", "/$indexName/_refresh"))

        installFence(indexName)
        val result = flushAndGetHandoffCheckpoint(indexName)

        assertThat((result["shard_count"] as Number).toInt()).isEqualTo(shardCount)

        @Suppress("UNCHECKED_CAST")
        val shards = result["shards"] as List<Map<String, Any>>
        assertThat(shards.map { it["shard"] as Int }).containsExactlyInAnyOrderElementsOf((0 until shardCount).toList())
        for (shard in shards) {
            assertThat(shard["primary"]).isEqualTo(true)
            val handoff = (shard["handoff_seq_no"] as Number).toLong()
            val maxSeq = (shard["max_seq_no"] as Number).toLong()
            // For zero-replica, handoff == max.
            assertThat(handoff).isEqualTo(maxSeq)
        }

        // Total docs across shards = docCount.
        val totalOps = shards.sumOf { (it["handoff_seq_no"] as Number).toLong() + 1 }
        assertThat(totalOps).isEqualTo(docCount.toLong())
    }

    fun `test handoff checkpoint without fence is refused`() {
        val leader = getClientForCluster(LEADER)
        val indexName = "unfenced_" + randomAlphaOfLength(6).lowercase(Locale.ROOT)

        val createRequest = CreateIndexRequest(indexName)
            .settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        assertThat(leader.indices().create(createRequest, RequestOptions.DEFAULT).isAcknowledged).isTrue()

        try {
            leader.lowLevelClient.performRequest(
                Request("POST", "/_plugins/_replication/switchover/$indexName/_flush_and_get_handoff_checkpoint")
            )
            fail<Unit>("Expected action to be refused when fence is not installed")
        } catch (e: ResponseException) {
            assertThat(e.response.statusLine.statusCode).isGreaterThanOrEqualTo(400)
            val body = String(e.response.entity.content.readAllBytes())
            assertThat(body).contains("fence")
        }
    }

    fun `test unfence removes the block`() {
        val leader = getClientForCluster(LEADER)
        val indexName = "unfence_" + randomAlphaOfLength(6).lowercase(Locale.ROOT)

        val createRequest = CreateIndexRequest(indexName)
            .settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        assertThat(leader.indices().create(createRequest, RequestOptions.DEFAULT).isAcknowledged).isTrue()

        installFence(indexName)

        try {
            performIndex(indexName, "blocked", """{"x":1}""")
            fail<Unit>("expected block")
        } catch (_: ResponseException) {
            // expected
        }

        val unfenceResponse = leader.lowLevelClient.performRequest(
            Request("DELETE", "/_plugins/_replication/switchover/$indexName/_fence")
        )
        assertThat(unfenceResponse.statusLine.statusCode).isEqualTo(200)

        assertThat(performIndex(indexName, "after_unfence", """{"x":2}""")).isIn(200, 201)
    }

    private fun installFence(indexName: String) {
        val leader = getClientForCluster(LEADER)
        val response = leader.lowLevelClient.performRequest(
            Request("POST", "/_plugins/_replication/switchover/$indexName/_fence")
        )
        fencedIndices.add(indexName)
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

    private fun performIndex(indexName: String, id: String, body: String): Int {
        val leader = getClientForCluster(LEADER)
        val req = Request("POST", "/$indexName/_doc/$id")
        req.entity = StringEntity(body, ContentType.APPLICATION_JSON)
        return leader.lowLevelClient.performRequest(req).statusLine.statusCode
    }
}
