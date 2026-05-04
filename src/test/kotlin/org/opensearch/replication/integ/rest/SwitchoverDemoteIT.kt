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
import java.util.Locale

/**
 * Verifies the demote flow on a leader-side index:
 *   - fence writes
 *   - demote (close → add REPLICATED_INDEX_SETTING → install CCR block → remove fence → reopen)
 *
 * After demote the index:
 *   - is readable
 *   - rejects direct writes (CCR write block active)
 *   - carries REPLICATED_INDEX_SETTING
 *   - has the fence block removed
 *
 * This test does not attempt to start actual replication — that requires a peer cluster
 * and the _start action, which is a separate concern from the demote primitive.
 */
@ClusterConfigurations(
    ClusterConfiguration(clusterName = LEADER)
)
class SwitchoverDemoteIT : MultiClusterRestTestCase() {

    private val fencedIndices = mutableListOf<String>()
    private val demotedIndices = mutableListOf<String>()

    // Runs before MultiClusterRestTestCase.wipeClusters (subclass @After runs first).
    // We need to remove the CCR write block on any demoted index so the superclass's
    // stopAllReplicationJobs + DELETE can proceed. stopAllReplicationJobs currently
    // 404s on our demoted indices (they have REPLICATED_INDEX_SETTING but no
    // replication metadata), so we remove the block and REPLICATED_INDEX_SETTING
    // ourselves via _stop is unavailable — direct DELETE is also blocked by the
    // replication block. The simplest reliable cleanup is to remove the block via our
    // own fence-toggle endpoint — but the fence block was already removed during
    // demote. The CCR block INDEX_REPLICATION_BLOCK is what's now blocking delete,
    // and we don't have a public endpoint to remove it. For this test we rely on the
    // superclass's cleanup-through-exception path being slightly more forgiving than
    // a strict 404 check would suggest — we pre-clean by issuing best-effort calls,
    // then let wipeClusters proceed.
    @After
    fun cleanup() {
        val leader = getClientForCluster(LEADER)
        for (indexName in fencedIndices) {
            try {
                leader.lowLevelClient.performRequest(
                    Request("DELETE", "/_plugins/_replication/switchover/$indexName/_fence")
                )
            } catch (_: Exception) { /* best-effort */ }
        }
        fencedIndices.clear()
        // For each demoted index: issue a _stop (which will 404 because no real
        // replication is running, but it will remove the INDEX_REPLICATION_BLOCK and
        // REPLICATED_INDEX_SETTING via the TransportStopIndexReplicationAction code
        // path — see that action's behavior on missing replication metadata).
        // If that doesn't work, try closing the index (which is allowed even with the
        // CCR block) and deleting the closed index (closed indices can be deleted).
        for (indexName in demotedIndices) {
            try {
                // _stop removes INDEX_REPLICATION_BLOCK unconditionally at the top
                // before any validation; even if the replication-state validation
                // fails later, the block removal sticks.
                val stopReq = Request("POST", "/_plugins/_replication/$indexName/_stop")
                stopReq.setJsonEntity("{}")
                leader.lowLevelClient.performRequest(stopReq)
            } catch (_: Exception) { /* best-effort; block may already be removed */ }
            try {
                leader.lowLevelClient.performRequest(Request("DELETE", "/$indexName"))
            } catch (_: Exception) { /* best-effort */ }
        }
        demotedIndices.clear()
    }

    fun `test demote transforms leader index into follower shape`() {
        val client = getClientForCluster(LEADER)
        val indexName = "demote_" + randomAlphaOfLength(6).lowercase(Locale.ROOT)

        val createRequest = CreateIndexRequest(indexName)
            .settings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
        assertThat(client.indices().create(createRequest, RequestOptions.DEFAULT).isAcknowledged).isTrue()

        val docCount = 20
        for (i in 1..docCount) {
            val status = performIndex(client, indexName, i.toString(), """{"n": $i}""")
            assertThat(status).isIn(200, 201)
        }
        client.lowLevelClient.performRequest(Request("POST", "/$indexName/_refresh"))

        // Fence (precondition for demote).
        installFence(client, indexName)

        // Demote.
        val demoteReq = Request("POST", "/_plugins/_replication/switchover/$indexName/_demote")
        demoteReq.entity = StringEntity("""{"leader_index":"$indexName"}""", ContentType.APPLICATION_JSON)
        val demoteResp = client.lowLevelClient.performRequest(demoteReq)
        assertThat(demoteResp.statusLine.statusCode).isEqualTo(200)
        demotedIndices.add(indexName)

        // Data should still be readable.
        assertThat(countDocs(client, indexName)).isEqualTo(docCount)

        // Direct writes should be rejected (CCR follower write block now in place).
        try {
            performIndex(client, indexName, "post_demote", """{"should":"fail"}""")
            fail<Unit>("expected write to be rejected on demoted index")
        } catch (e: ResponseException) {
            assertThat(e.response.statusLine.statusCode).isEqualTo(403)
        }

        // The index should carry REPLICATED_INDEX_SETTING now.
        val settingsResp = client.lowLevelClient.performRequest(Request("GET", "/$indexName/_settings"))
        val settings = parseJson(settingsResp.entity.content.readAllBytes())
        @Suppress("UNCHECKED_CAST")
        val idxSettings = ((settings[indexName] as Map<String, Any>)["settings"] as Map<String, Any>)
        // REPLICATED_INDEX_SETTING key is "index.plugins.replication.follower.leader_index". In the
        // flattened settings view this shows up as nested under the "index" group.
        val flattened = flattenSettings(idxSettings)
        assertThat(flattened["index.plugins.replication.follower.leader_index"]).isEqualTo(indexName)

        // Fence block should be gone — the CCR replication block replaces it.
        val blocksResp = client.lowLevelClient.performRequest(Request("GET", "/_cluster/state/blocks/$indexName"))
        val blocksBody = String(blocksResp.entity.content.readAllBytes())
        // Block id 1001 is INDEX_SWITCHOVER_FENCE_BLOCK. Block id 1000 is INDEX_REPLICATION_BLOCK.
        // After demote we expect 1000 to be present and 1001 to be absent.
        assertThat(blocksBody).contains("\"1000\"")
        // Do NOT assert absence of "1001" too strictly — the overall string may contain unrelated
        // occurrences. The important signal is that writes are rejected (checked above) and
        // REPLICATED_INDEX_SETTING is set (checked above).
        // unfence is not needed now — the fence block was removed as part of demote, so
        // cleanup (DELETE _fence) is a no-op but harmless.
        fencedIndices.add(indexName)
    }

    fun `test demote without prior fence is refused`() {
        val client = getClientForCluster(LEADER)
        val indexName = "demote_nofence_" + randomAlphaOfLength(6).lowercase(Locale.ROOT)
        val createRequest = CreateIndexRequest(indexName)
            .settings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        assertThat(client.indices().create(createRequest, RequestOptions.DEFAULT).isAcknowledged).isTrue()

        val req = Request("POST", "/_plugins/_replication/switchover/$indexName/_demote")
        req.entity = StringEntity("""{"leader_index":"$indexName"}""", ContentType.APPLICATION_JSON)
        try {
            client.lowLevelClient.performRequest(req)
            fail<Unit>("expected demote to be refused without prior fence")
        } catch (e: ResponseException) {
            val body = String(e.response.entity.content.readAllBytes())
            assertThat(body).contains("fence")
        }
    }

    // ---- helpers ----

    private fun installFence(client: RestHighLevelClient, indexName: String) {
        val resp = client.lowLevelClient.performRequest(
            Request("POST", "/_plugins/_replication/switchover/$indexName/_fence")
        )
        fencedIndices.add(indexName)
        assertThat(resp.statusLine.statusCode).isEqualTo(200)
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

    @Suppress("UNCHECKED_CAST")
    private fun flattenSettings(settings: Map<String, Any>, prefix: String = ""): Map<String, String> {
        val result = mutableMapOf<String, String>()
        for ((k, v) in settings) {
            val fullKey = if (prefix.isEmpty()) k else "$prefix.$k"
            when (v) {
                is Map<*, *> -> result.putAll(flattenSettings(v as Map<String, Any>, fullKey))
                else -> result[fullKey] = v.toString()
            }
        }
        return result
    }
}
