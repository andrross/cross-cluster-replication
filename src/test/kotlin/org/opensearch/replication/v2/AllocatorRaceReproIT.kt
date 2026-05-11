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
import org.apache.hc.core5.http.io.entity.StringEntity
import org.assertj.core.api.Assertions.assertThat
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.replication.MultiClusterAnnotations.ClusterConfiguration
import org.opensearch.replication.MultiClusterAnnotations.ClusterConfigurations
import org.opensearch.replication.MultiClusterRestTestCase
import org.junit.Rule
import org.junit.rules.Timeout
import java.util.Locale
import java.util.concurrent.TimeUnit

private const val L = "leaderCluster"
private const val F = "followCluster"

/**
 * Minimal reproduction for the PrimaryShardAllocator assertion crash observed when intent is
 * set before any user index exists on the leader, then an index is created.
 *
 * preserveIndices is not needed here: this file contains one test; the suite is only invoked
 * in isolation.
 */
@ClusterConfigurations(
    ClusterConfiguration(clusterName = L, preserveIndices = true),
    ClusterConfiguration(clusterName = F, preserveIndices = true)
)
class AllocatorRaceReproIT : MultiClusterRestTestCase() {

    @get:Rule
    val timeout: Timeout = Timeout(90, TimeUnit.SECONDS)

    fun `test repro - intent set before leader index exists`() {
        val leader = getClientForCluster(L)
        createConnectionBetweenClusters(F, L, connectionName = "source")

        putIntent(L, F, "PRIMARY")
        putIntent(F, "source", "SECONDARY")

        val name = "repro-${randomAlphaOfLength(6).lowercase(Locale.ROOT)}"
        leader.indices().create(CreateIndexRequest(name), RequestOptions.DEFAULT)
        leader.index(IndexRequest(name).id("1").source(mapOf("k" to "v")), RequestOptions.DEFAULT)

        Thread.sleep(10_000)

        val resp = getNamedCluster(F).lowLevelClient.performRequest(Request("GET", "/_cluster/health"))
        assertThat(resp.statusLine.statusCode).isEqualTo(200)
    }

    fun `test repro - prior restore then intent-first again`() {
        val leader = getClientForCluster(L)
        createConnectionBetweenClusters(F, L, connectionName = "source")

        // Phase 1: index-first pattern (known-good)
        val a = "repro-a-${randomAlphaOfLength(6).lowercase(Locale.ROOT)}"
        leader.indices().create(CreateIndexRequest(a), RequestOptions.DEFAULT)
        leader.index(IndexRequest(a).id("1").source(mapOf("k" to "a")), RequestOptions.DEFAULT)

        putIntent(L, F, "PRIMARY")
        putIntent(F, "source", "SECONDARY")

        Thread.sleep(8_000)

        // Clear intent — this is what happens between tests in the suite.
        clearIntent(F, "source")
        clearIntent(L, F)
        Thread.sleep(2_000)

        // Phase 2: intent-first pattern (crash candidate)
        putIntent(L, F, "PRIMARY")
        putIntent(F, "source", "SECONDARY")
        Thread.sleep(1_000)

        val b = "repro-b-${randomAlphaOfLength(6).lowercase(Locale.ROOT)}"
        leader.indices().create(CreateIndexRequest(b), RequestOptions.DEFAULT)
        leader.index(IndexRequest(b).id("1").source(mapOf("k" to "b")), RequestOptions.DEFAULT)

        Thread.sleep(10_000)

        val resp = getNamedCluster(F).lowLevelClient.performRequest(Request("GET", "/_cluster/health"))
        assertThat(resp.statusLine.statusCode).isEqualTo(200)
    }

    private fun putIntent(cluster: String, peer: String, role: String) {
        val body = """{"role":"$role","epoch":1,"status":"STEADY"}"""
        val req = Request("PUT", "/_replication/cluster/$peer")
        req.entity = StringEntity(body, ContentType.APPLICATION_JSON)
        getNamedCluster(cluster).lowLevelClient.performRequest(req)
    }

    private fun clearIntent(cluster: String, peer: String) {
        val req = Request("DELETE", "/_replication/cluster/$peer")
        getNamedCluster(cluster).lowLevelClient.performRequest(req)
    }
}
