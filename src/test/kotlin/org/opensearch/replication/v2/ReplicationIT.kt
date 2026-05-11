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

package org.opensearch.replication.v2

import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.HttpStatus
import org.apache.hc.core5.http.io.entity.StringEntity
import org.assertj.core.api.Assertions.assertThat
import org.opensearch.action.delete.DeleteRequest
import org.opensearch.action.get.GetRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.action.ingest.GetPipelineRequest
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetComponentTemplatesRequest
import org.opensearch.client.indices.GetComposableIndexTemplateRequest
import org.opensearch.client.indices.GetIndexTemplatesRequest
import org.opensearch.replication.MultiClusterAnnotations.ClusterConfiguration
import org.opensearch.replication.MultiClusterAnnotations.ClusterConfigurations
import org.opensearch.replication.MultiClusterRestTestCase
import org.opensearch.test.OpenSearchTestCase.assertBusy
import org.junit.After
import org.junit.Rule
import org.junit.rules.Timeout
import java.util.Locale
import java.util.concurrent.TimeUnit

private const val LEADER = "leaderCluster"
private const val FOLLOWER = "followCluster"
private const val REMOTE_ALIAS = "source"

/**
 * End-to-end tests for the replication control plane. Covers:
 *  - Initial bootstrap + ops replication on a clean index.
 *  - Update/delete docs on the leader propagate.
 *  - Ingest-pipeline upsert/update/delete through the metadata controller.
 *  - Index created on leader after intent is set.
 *  - Delete path: leader-side index delete removes follower index; clear-intent removes all.
 *  - Component + composable + v1 templates upsert/delete.
 *
 * Each test sets up its own intent and clears both sides in @After. Clearing the secondary's
 * intent triggers the orchestrator's bulk delete of follower indices, which is the same path
 * exercised by the delete-path test. No preserveIndices flag is needed.
 */
@ClusterConfigurations(
    ClusterConfiguration(clusterName = LEADER),
    ClusterConfiguration(clusterName = FOLLOWER)
)
class ReplicationIT : MultiClusterRestTestCase() {

    @get:Rule
    val timeout: Timeout = Timeout(180, TimeUnit.SECONDS)

    @After
    fun clearIntents() {
        runCatching { clearReplicationIntent(FOLLOWER, REMOTE_ALIAS) }
        runCatching { clearReplicationIntent(LEADER, FOLLOWER) }
    }

    fun `test data plane - initial bootstrap and ops replication`() {
        val leader = getClientForCluster(LEADER)
        val follower = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionName = REMOTE_ALIAS)

        val indexName = randomIndexName()
        leader.indices().create(CreateIndexRequest(indexName), RequestOptions.DEFAULT)

        val source = mapOf("name" to randomAlphaOfLength(20), "age" to randomInt().toString())
        leader.index(IndexRequest(indexName).id("1").source(source), RequestOptions.DEFAULT)

        putIntents()

        awaitFollowerDoc(follower, indexName, "1", source)

        for (i in 2..5) {
            leader.index(IndexRequest(indexName).id("$i").source(source), RequestOptions.DEFAULT)
        }
        assertBusy({
            for (i in 2..5) {
                assertThat(tryGet(follower, indexName, "$i"))
                    .withFailMessage("follower missing doc $i for $indexName")
                    .isNotNull()
            }
        }, 30L, TimeUnit.SECONDS)
    }

    fun `test data plane - updates and deletes propagate`() {
        val leader = getClientForCluster(LEADER)
        val follower = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionName = REMOTE_ALIAS)

        val indexName = randomIndexName()
        leader.indices().create(CreateIndexRequest(indexName), RequestOptions.DEFAULT)

        val v1 = mapOf("msg" to "hello", "n" to "1")
        leader.index(IndexRequest(indexName).id("doc").source(v1), RequestOptions.DEFAULT)

        putIntents()
        awaitFollowerDoc(follower, indexName, "doc", v1)

        val v2 = mapOf("msg" to "hello", "n" to "2")
        leader.index(IndexRequest(indexName).id("doc").source(v2), RequestOptions.DEFAULT)
        awaitFollowerDoc(follower, indexName, "doc", v2)

        leader.delete(DeleteRequest(indexName, "doc"), RequestOptions.DEFAULT)
        assertBusy({
            assertThat(tryGet(follower, indexName, "doc"))
                .withFailMessage("follower should have deleted doc")
                .isNull()
        }, 30L, TimeUnit.SECONDS)
    }

    fun `test metadata - ingest pipeline upsert update and delete`() {
        val leader = getClientForCluster(LEADER)
        val follower = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionName = REMOTE_ALIAS)

        val indexName = randomIndexName()
        leader.indices().create(CreateIndexRequest(indexName), RequestOptions.DEFAULT)
        putIntents()
        val sentinel = mapOf("k" to "v")
        leader.index(IndexRequest(indexName).id("1").source(sentinel), RequestOptions.DEFAULT)
        awaitFollowerDoc(follower, indexName, "1", sentinel)

        val pipelineId = "v2-pipeline-${randomAlphaOfLength(6).lowercase(Locale.ROOT)}"

        putPipeline(LEADER, pipelineId, """{"description":"v1","processors":[{"set":{"field":"foo","value":"bar"}}]}""")
        assertBusy({
            assertThat(getPipelineDescription(follower, pipelineId))
                .withFailMessage("follower should see pipeline $pipelineId")
                .isEqualTo("v1")
        }, 30L, TimeUnit.SECONDS)

        putPipeline(LEADER, pipelineId, """{"description":"v2","processors":[{"set":{"field":"foo","value":"baz"}}]}""")
        assertBusy({
            assertThat(getPipelineDescription(follower, pipelineId))
                .withFailMessage("follower should see updated pipeline description")
                .isEqualTo("v2")
        }, 30L, TimeUnit.SECONDS)

        deletePipeline(LEADER, pipelineId)
        assertBusy({
            assertThat(getPipelineDescription(follower, pipelineId))
                .withFailMessage("follower should have deleted pipeline")
                .isNull()
        }, 30L, TimeUnit.SECONDS)
    }

    fun `test metadata - component template upsert and delete`() {
        val leader = getClientForCluster(LEADER)
        val follower = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionName = REMOTE_ALIAS)

        val indexName = randomIndexName()
        leader.indices().create(CreateIndexRequest(indexName), RequestOptions.DEFAULT)
        putIntents()
        leader.index(IndexRequest(indexName).id("1").source(mapOf("k" to "v")), RequestOptions.DEFAULT)
        awaitFollowerDoc(follower, indexName, "1", mapOf("k" to "v"))

        val templateName = "v2-ct-${randomAlphaOfLength(6).lowercase(Locale.ROOT)}"
        putComponentTemplate(LEADER, templateName, """{"template":{"settings":{"number_of_shards":1}}}""")
        assertBusy({
            assertThat(componentTemplateExists(follower, templateName))
                .withFailMessage("follower should see component template $templateName")
                .isTrue()
        }, 30L, TimeUnit.SECONDS)

        deleteComponentTemplate(LEADER, templateName)
        assertBusy({
            assertThat(componentTemplateExists(follower, templateName))
                .withFailMessage("follower should not have component template $templateName")
                .isFalse()
        }, 30L, TimeUnit.SECONDS)
    }

    fun `test metadata - composable index template upsert and delete`() {
        val leader = getClientForCluster(LEADER)
        val follower = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionName = REMOTE_ALIAS)

        val indexName = randomIndexName()
        leader.indices().create(CreateIndexRequest(indexName), RequestOptions.DEFAULT)
        putIntents()
        leader.index(IndexRequest(indexName).id("1").source(mapOf("k" to "v")), RequestOptions.DEFAULT)
        awaitFollowerDoc(follower, indexName, "1", mapOf("k" to "v"))

        val templateName = "v2-cit-${randomAlphaOfLength(6).lowercase(Locale.ROOT)}"
        val patternName = "v2-cit-pat-${randomAlphaOfLength(6).lowercase(Locale.ROOT)}"
        putComposableIndexTemplate(
            LEADER, templateName,
            """{"index_patterns":["$patternName-*"],"template":{"settings":{"number_of_shards":1}},"priority":500}"""
        )
        assertBusy({
            assertThat(composableIndexTemplateExists(follower, templateName))
                .withFailMessage("follower should see composable template $templateName")
                .isTrue()
        }, 30L, TimeUnit.SECONDS)

        deleteComposableIndexTemplate(LEADER, templateName)
        assertBusy({
            assertThat(composableIndexTemplateExists(follower, templateName))
                .withFailMessage("follower should not have composable template $templateName")
                .isFalse()
        }, 30L, TimeUnit.SECONDS)
    }

    fun `test metadata - v1 index template upsert and delete`() {
        val leader = getClientForCluster(LEADER)
        val follower = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionName = REMOTE_ALIAS)

        val indexName = randomIndexName()
        leader.indices().create(CreateIndexRequest(indexName), RequestOptions.DEFAULT)
        putIntents()
        leader.index(IndexRequest(indexName).id("1").source(mapOf("k" to "v")), RequestOptions.DEFAULT)
        awaitFollowerDoc(follower, indexName, "1", mapOf("k" to "v"))

        val templateName = "v2-v1t-${randomAlphaOfLength(6).lowercase(Locale.ROOT)}"
        val patternName = "v2-v1t-pat-${randomAlphaOfLength(6).lowercase(Locale.ROOT)}"
        putV1IndexTemplate(
            LEADER, templateName,
            """{"index_patterns":["$patternName-*"],"order":0,"settings":{"number_of_shards":1}}"""
        )
        assertBusy({
            assertThat(v1IndexTemplateExists(follower, templateName))
                .withFailMessage("follower should see v1 template $templateName")
                .isTrue()
        }, 30L, TimeUnit.SECONDS)

        deleteV1IndexTemplate(LEADER, templateName)
        assertBusy({
            assertThat(v1IndexTemplateExists(follower, templateName))
                .withFailMessage("follower should not have v1 template $templateName")
                .isFalse()
        }, 30L, TimeUnit.SECONDS)
    }

    fun `test intent - index created on leader after intent is set`() {
        val leader = getClientForCluster(LEADER)
        val follower = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionName = REMOTE_ALIAS)

        putIntents()

        val indexName = randomIndexName()
        leader.indices().create(CreateIndexRequest(indexName), RequestOptions.DEFAULT)
        val doc = mapOf("k" to "late")
        leader.index(IndexRequest(indexName).id("1").source(doc), RequestOptions.DEFAULT)

        awaitFollowerDoc(follower, indexName, "1", doc)
    }

    fun `test delete - leader-side index delete removes follower index`() {
        val leader = getClientForCluster(LEADER)
        val follower = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionName = REMOTE_ALIAS)

        val indexName = randomIndexName()
        leader.indices().create(CreateIndexRequest(indexName), RequestOptions.DEFAULT)
        leader.index(IndexRequest(indexName).id("1").source(mapOf("k" to "v")), RequestOptions.DEFAULT)

        putIntents()
        awaitFollowerDoc(follower, indexName, "1", mapOf("k" to "v"))

        // Delete on leader. Bootstrap sweep should notice the primary no longer has it and
        // remove it on the follower.
        val delReq = Request("DELETE", "/$indexName")
        getNamedCluster(LEADER).lowLevelClient.performRequest(delReq)

        assertBusy({
            assertThat(indexExists(follower, indexName))
                .withFailMessage("follower should have deleted $indexName")
                .isFalse()
        }, 30L, TimeUnit.SECONDS)
    }

    fun `test delete - clearing secondary intent removes all follower indices`() {
        val leader = getClientForCluster(LEADER)
        val follower = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionName = REMOTE_ALIAS)

        val a = randomIndexName()
        val b = randomIndexName()
        leader.indices().create(CreateIndexRequest(a), RequestOptions.DEFAULT)
        leader.indices().create(CreateIndexRequest(b), RequestOptions.DEFAULT)
        leader.index(IndexRequest(a).id("1").source(mapOf("x" to "1")), RequestOptions.DEFAULT)
        leader.index(IndexRequest(b).id("1").source(mapOf("x" to "2")), RequestOptions.DEFAULT)

        putIntents()
        awaitFollowerDoc(follower, a, "1", mapOf("x" to "1"))
        awaitFollowerDoc(follower, b, "1", mapOf("x" to "2"))

        clearReplicationIntent(FOLLOWER, REMOTE_ALIAS)

        assertBusy({
            assertThat(indexExists(follower, a))
                .withFailMessage("follower should have deleted $a after clearing intent")
                .isFalse()
            assertThat(indexExists(follower, b))
                .withFailMessage("follower should have deleted $b after clearing intent")
                .isFalse()
        }, 30L, TimeUnit.SECONDS)
    }

    // --- helpers ---------------------------------------------------------------------------

    private fun randomIndexName(): String = randomAlphaOfLength(10).lowercase(Locale.ROOT)

    private fun putIntents() {
        putReplicationIntent(LEADER, FOLLOWER, "PRIMARY")
        putReplicationIntent(FOLLOWER, REMOTE_ALIAS, "SECONDARY")
    }

    private fun awaitFollowerDoc(
        client: RestHighLevelClient,
        index: String,
        id: String,
        expected: Map<String, Any>,
        seconds: Long = 45L
    ) {
        assertBusy({
            val got = tryGet(client, index, id)
            assertThat(got)
                .withFailMessage("follower has not yet received doc $id for $index (got $got)")
                .isEqualTo(expected)
        }, seconds, TimeUnit.SECONDS)
    }

    private fun tryGet(client: RestHighLevelClient, index: String, id: String): Map<String, Any>? {
        return try {
            val resp = client.get(GetRequest(index, id), RequestOptions.DEFAULT)
            if (resp.isExists) resp.sourceAsMap else null
        } catch (e: Exception) {
            null
        }
    }

    private fun indexExists(client: RestHighLevelClient, index: String): Boolean {
        return try {
            client.indices().exists(
                org.opensearch.client.indices.GetIndexRequest(index),
                RequestOptions.DEFAULT
            )
        } catch (e: Exception) {
            false
        }
    }

    private fun putPipeline(cluster: String, id: String, body: String) {
        val req = Request("PUT", "/_ingest/pipeline/$id")
        req.entity = StringEntity(body, ContentType.APPLICATION_JSON)
        val resp = getNamedCluster(cluster).lowLevelClient.performRequest(req)
        assertThat(resp.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    }

    private fun deletePipeline(cluster: String, id: String) {
        val req = Request("DELETE", "/_ingest/pipeline/$id")
        val resp = getNamedCluster(cluster).lowLevelClient.performRequest(req)
        assertThat(resp.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    }

    private fun getPipelineDescription(client: RestHighLevelClient, id: String): String? {
        val req = GetPipelineRequest(id)
        return try {
            val resp = client.ingest().getPipeline(req, RequestOptions.DEFAULT)
            val cfg = resp.pipelines().firstOrNull { it.id == id } ?: return null
            cfg.configAsMap["description"] as String?
        } catch (e: Exception) {
            null
        }
    }

    private fun putComponentTemplate(cluster: String, name: String, body: String) {
        val req = Request("PUT", "/_component_template/$name")
        req.entity = StringEntity(body, ContentType.APPLICATION_JSON)
        val resp = getNamedCluster(cluster).lowLevelClient.performRequest(req)
        assertThat(resp.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    }

    private fun deleteComponentTemplate(cluster: String, name: String) {
        val req = Request("DELETE", "/_component_template/$name")
        val resp = getNamedCluster(cluster).lowLevelClient.performRequest(req)
        assertThat(resp.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    }

    private fun componentTemplateExists(client: RestHighLevelClient, name: String): Boolean {
        return try {
            val resp = client.cluster().getComponentTemplate(
                GetComponentTemplatesRequest(name),
                RequestOptions.DEFAULT
            )
            resp.componentTemplates.containsKey(name)
        } catch (e: Exception) {
            false
        }
    }

    private fun putComposableIndexTemplate(cluster: String, name: String, body: String) {
        val req = Request("PUT", "/_index_template/$name")
        req.entity = StringEntity(body, ContentType.APPLICATION_JSON)
        val resp = getNamedCluster(cluster).lowLevelClient.performRequest(req)
        assertThat(resp.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    }

    private fun deleteComposableIndexTemplate(cluster: String, name: String) {
        val req = Request("DELETE", "/_index_template/$name")
        val resp = getNamedCluster(cluster).lowLevelClient.performRequest(req)
        assertThat(resp.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    }

    private fun composableIndexTemplateExists(client: RestHighLevelClient, name: String): Boolean {
        return try {
            val resp = client.indices().getIndexTemplate(
                GetComposableIndexTemplateRequest(name),
                RequestOptions.DEFAULT
            )
            resp.indexTemplates.containsKey(name)
        } catch (e: Exception) {
            false
        }
    }

    private fun putV1IndexTemplate(cluster: String, name: String, body: String) {
        val req = Request("PUT", "/_template/$name")
        req.entity = StringEntity(body, ContentType.APPLICATION_JSON)
        val resp = getNamedCluster(cluster).lowLevelClient.performRequest(req)
        assertThat(resp.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    }

    private fun deleteV1IndexTemplate(cluster: String, name: String) {
        val req = Request("DELETE", "/_template/$name")
        val resp = getNamedCluster(cluster).lowLevelClient.performRequest(req)
        assertThat(resp.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    }

    private fun v1IndexTemplateExists(client: RestHighLevelClient, name: String): Boolean {
        return try {
            val resp = client.indices().getIndexTemplate(
                GetIndexTemplatesRequest(listOf(name)),
                RequestOptions.DEFAULT
            )
            resp.indexTemplates.any { it.name() == name }
        } catch (e: Exception) {
            false
        }
    }

    private fun putReplicationIntent(cluster: String, peerAlias: String, role: String) {
        val body = """{"role":"$role","epoch":1,"status":"STEADY"}"""
        val req = Request("PUT", "/_replication/cluster/$peerAlias")
        req.entity = StringEntity(body, ContentType.APPLICATION_JSON)
        val resp = getNamedCluster(cluster).lowLevelClient.performRequest(req)
        assertThat(resp.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    }

    private fun clearReplicationIntent(cluster: String, peerAlias: String) {
        val req = Request("DELETE", "/_replication/cluster/$peerAlias")
        getNamedCluster(cluster).lowLevelClient.performRequest(req)
    }
}
