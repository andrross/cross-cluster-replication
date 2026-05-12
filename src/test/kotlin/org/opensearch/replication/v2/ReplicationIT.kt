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

// Customer-chosen, stable across the life of the replication relationship. Both sides
// address the intent under this ID.
private const val RELATIONSHIP_ID = "test-rel"

// The `cluster.remote.<alias>` connection name on the follower (set by
// `createConnectionBetweenClusters`) — also the follower intent's `remote_alias`.
private const val REMOTE_ALIAS = "source"

// Identity labels; `remote_alias` on the leader is cosmetic because the leader never calls
// back through it, but we still carry it through for symmetry.
private const val FOLLOWER_LOCAL_ALIAS = "follower"
private const val LEADER_LOCAL_ALIAS = "leader"

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
        runCatching { clearReplicationIntent(FOLLOWER, RELATIONSHIP_ID) }
        runCatching { clearReplicationIntent(LEADER, RELATIONSHIP_ID) }
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

    fun `test intent - GET returns the configured intent`() {
        // Before setup: 404.
        val (preStatus, preBody) = getReplicationIntent(FOLLOWER, RELATIONSHIP_ID)
        assertThat(preStatus).isEqualTo(HttpStatus.SC_NOT_FOUND)
        assertThat(preBody).isNull()

        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionName = REMOTE_ALIAS)
        putIntents()

        val (status, body) = getReplicationIntent(FOLLOWER, RELATIONSHIP_ID)
        assertThat(status).isEqualTo(HttpStatus.SC_OK)
        assertThat(body).isNotNull()
        assertThat(body!!["relationship_id"]).isEqualTo(RELATIONSHIP_ID)
        assertThat(body["local_alias"]).isEqualTo(FOLLOWER_LOCAL_ALIAS)
        assertThat(body["remote_alias"]).isEqualTo(REMOTE_ALIAS)
        assertThat(body["role"]).isEqualTo("SECONDARY")
        assertThat((body["epoch"] as Number).toLong()).isEqualTo(1L)
        assertThat(body["status"]).isEqualTo("STEADY")

        // Auto-install writes the mirrored PRIMARY intent on the leader.
        val (leaderStatus, leaderBody) = getReplicationIntent(LEADER, RELATIONSHIP_ID)
        assertThat(leaderStatus).isEqualTo(HttpStatus.SC_OK)
        assertThat(leaderBody!!["relationship_id"]).isEqualTo(RELATIONSHIP_ID)
        assertThat(leaderBody["role"]).isEqualTo("PRIMARY")
        assertThat(leaderBody["local_alias"]).isEqualTo(REMOTE_ALIAS)
        assertThat(leaderBody["remote_alias"]).isEqualTo(FOLLOWER_LOCAL_ALIAS)

        // Mismatched relationship_id: 404 even though an intent is configured under a
        // different ID.
        val (mismatchStatus, _) = getReplicationIntent(FOLLOWER, "no-such-rel")
        assertThat(mismatchStatus).isEqualTo(HttpStatus.SC_NOT_FOUND)

        // After DELETE: 404 again.
        clearReplicationIntent(FOLLOWER, RELATIONSHIP_ID)
        val (postStatus, _) = getReplicationIntent(FOLLOWER, RELATIONSHIP_ID)
        assertThat(postStatus).isEqualTo(HttpStatus.SC_NOT_FOUND)
    }

    fun `test status - reports loop active and metadata version`() {
        val leader = getClientForCluster(LEADER)
        val follower = getClientForCluster(FOLLOWER)
        createConnectionBetweenClusters(FOLLOWER, LEADER, connectionName = REMOTE_ALIAS)

        // Before setup: 404.
        val (preStatus, _) = getReplicationStatus(FOLLOWER, RELATIONSHIP_ID)
        assertThat(preStatus).isEqualTo(HttpStatus.SC_NOT_FOUND)

        // Bring up an index on the leader so the secondary's long-poll has something to apply
        // and advance last_applied_metadata_version past 0.
        val indexName = randomIndexName()
        leader.indices().create(CreateIndexRequest(indexName), RequestOptions.DEFAULT)
        leader.index(IndexRequest(indexName).id("1").source(mapOf("k" to "v")), RequestOptions.DEFAULT)

        putIntents()
        awaitFollowerDoc(follower, indexName, "1", mapOf("k" to "v"))

        // By now the long-poll loop has been running and has applied at least one cluster-state
        // version.
        assertBusy({
            val (code, body) = getReplicationStatus(FOLLOWER, RELATIONSHIP_ID)
            assertThat(code).isEqualTo(HttpStatus.SC_OK)
            assertThat(body).isNotNull()
            assertThat(body!!["relationship_id"]).isEqualTo(RELATIONSHIP_ID)
            assertThat(body["local_alias"]).isEqualTo(FOLLOWER_LOCAL_ALIAS)
            assertThat(body["remote_alias"]).isEqualTo(REMOTE_ALIAS)
            assertThat(body["loop_active"]).isEqualTo(true)
            assertThat((body["last_applied_metadata_version"] as Number).toLong()).isGreaterThan(0L)
        }, 30L, TimeUnit.SECONDS)

        // Mismatched relationship_id: 404.
        val (mismatchStatus, _) = getReplicationStatus(FOLLOWER, "no-such-rel")
        assertThat(mismatchStatus).isEqualTo(HttpStatus.SC_NOT_FOUND)
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

    fun `test delete - clearing secondary intent preserves data and makes indices writable`() {
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

        clearReplicationIntent(FOLLOWER, RELATIONSHIP_ID)

        // Indices still exist, data is preserved.
        assertBusy({
            assertThat(indexExists(follower, a))
                .withFailMessage("follower should have preserved $a after clearing intent")
                .isTrue()
            assertThat(indexExists(follower, b))
                .withFailMessage("follower should have preserved $b after clearing intent")
                .isTrue()
            assertThat(tryGet(follower, a, "1"))
                .withFailMessage("follower should have preserved doc in $a")
                .isEqualTo(mapOf("x" to "1"))
            assertThat(tryGet(follower, b, "1"))
                .withFailMessage("follower should have preserved doc in $b")
                .isEqualTo(mapOf("x" to "2"))
        }, 30L, TimeUnit.SECONDS)

        // Follower-marker is stripped; indices are now writable on what used to be the
        // secondary. Indexing a fresh doc should succeed.
        val resp = follower.index(
            IndexRequest(a).id("post-sever").source(mapOf("written-on" to "former-secondary")),
            RequestOptions.DEFAULT
        )
        assertThat(resp.result.name).isIn("CREATED", "UPDATED")
    }

    // --- helpers ---------------------------------------------------------------------------

    private fun randomIndexName(): String = randomAlphaOfLength(10).lowercase(Locale.ROOT)

    private fun putIntents() {
        // Only one PUT: the SECONDARY-side handler auto-installs the matching PRIMARY
        // intent on the leader via the remote-cluster connection. No manual leader-side PUT.
        putSecondaryIntent(FOLLOWER, RELATIONSHIP_ID)
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

    /**
     * Issue a SECONDARY-role PUT to `cluster` under `relationshipId`. The handler auto-installs
     * the matching PRIMARY intent on the peer via `cluster.remote.<REMOTE_ALIAS>`, so callers
     * should not also PUT on the leader.
     */
    private fun putSecondaryIntent(cluster: String, relationshipId: String) {
        val body = """{
            |"role":"SECONDARY",
            |"local_alias":"$FOLLOWER_LOCAL_ALIAS",
            |"remote_alias":"$REMOTE_ALIAS",
            |"epoch":1,
            |"status":"STEADY"
            |}""".trimMargin()
        val req = Request("PUT", "/_replication/cluster/$relationshipId")
        req.entity = StringEntity(body, ContentType.APPLICATION_JSON)
        val resp = getNamedCluster(cluster).lowLevelClient.performRequest(req)
        assertThat(resp.statusLine.statusCode).isEqualTo(HttpStatus.SC_OK)
    }

    private fun clearReplicationIntent(cluster: String, relationshipId: String) {
        val req = Request("DELETE", "/_replication/cluster/$relationshipId")
        getNamedCluster(cluster).lowLevelClient.performRequest(req)
    }

    private fun getReplicationIntent(cluster: String, relationshipId: String): Pair<Int, Map<String, Any>?> {
        return restGetJson(cluster, "/_replication/cluster/$relationshipId")
    }

    private fun getReplicationStatus(cluster: String, relationshipId: String): Pair<Int, Map<String, Any>?> {
        return restGetJson(cluster, "/_replication/cluster/$relationshipId/status")
    }

    private fun restGetJson(cluster: String, path: String): Pair<Int, Map<String, Any>?> {
        val req = Request("GET", path)
        val client = getNamedCluster(cluster).lowLevelClient
        val resp = try {
            client.performRequest(req)
        } catch (e: org.opensearch.client.ResponseException) {
            return e.response.statusLine.statusCode to null
        }
        val code = resp.statusLine.statusCode
        if (code != HttpStatus.SC_OK) return code to null
        val body = org.apache.hc.core5.http.io.entity.EntityUtils.toString(resp.entity)
        val parsed = org.opensearch.common.xcontent.XContentType.JSON.xContent()
            .createParser(
                org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS,
                body
            )
            .map()
        return code to parsed
    }
}
