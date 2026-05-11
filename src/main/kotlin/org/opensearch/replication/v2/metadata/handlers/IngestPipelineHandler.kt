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

package org.opensearch.replication.v2.metadata.handlers

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ingest.DeletePipelineAction
import org.opensearch.action.ingest.DeletePipelineRequest
import org.opensearch.action.ingest.PutPipelineAction
import org.opensearch.action.ingest.PutPipelineRequest
import org.opensearch.cluster.ClusterState
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.core.common.bytes.BytesReference
import org.opensearch.ingest.IngestMetadata
import org.opensearch.ingest.PipelineConfiguration
import org.opensearch.replication.v2.metadata.ApplyResult
import org.opensearch.replication.v2.metadata.CategoryHandler
import org.opensearch.transport.client.Client

/**
 * Real ingest-pipeline handler: extracts from Metadata.customs[IngestMetadata.TYPE], applies
 * upserts via PutPipelineAction and deletes via DeletePipelineAction against the local cluster.
 *
 * `equal()` delegates to PipelineConfiguration.equals, which compares id + config map.
 *
 * The handler runs inside the single-threaded apply pipeline on the cluster manager, so
 * blocking actionGet() is acceptable here — the controller explicitly awaits each apply
 * result before moving on.
 */
class IngestPipelineHandler(private val client: Client) : CategoryHandler<PipelineConfiguration> {

    private val log = LogManager.getLogger(javaClass)

    override fun category(): String = "ingest_pipelines"

    override fun extract(clusterState: ClusterState): Map<String, PipelineConfiguration> {
        val ingest = clusterState.metadata.custom<IngestMetadata>(IngestMetadata.TYPE) ?: return emptyMap()
        return ingest.pipelines
    }

    override fun upsert(name: String, primary: PipelineConfiguration): ApplyResult {
        val source: BytesReference = try {
            // PipelineConfiguration.getConfig() is package-private, so re-serialize the map.
            val builder = XContentFactory.jsonBuilder()
            builder.map(primary.configAsMap)
            BytesReference.bytes(builder)
        } catch (e: Exception) {
            return ApplyResult.PermanentFailure(
                "failed to serialize pipeline config for $name: ${e.message}", e
            )
        }
        val req = PutPipelineRequest(name, source, XContentType.JSON)
        return try {
            val resp = client.execute(PutPipelineAction.INSTANCE, req).actionGet()
            if (resp.isAcknowledged) {
                log.debug("ingest_pipelines: upsert {} applied", name)
                ApplyResult.Success
            } else {
                ApplyResult.TransientFailure("PutPipeline not acknowledged for $name")
            }
        } catch (e: Exception) {
            ApplyResult.TransientFailure("PutPipeline failed for $name: ${e.message}", e)
        }
    }

    override fun delete(name: String): ApplyResult {
        val req = DeletePipelineRequest(name)
        return try {
            val resp = client.execute(DeletePipelineAction.INSTANCE, req).actionGet()
            if (resp.isAcknowledged) {
                log.debug("ingest_pipelines: delete {} applied", name)
                ApplyResult.Success
            } else {
                ApplyResult.TransientFailure("DeletePipeline not acknowledged for $name")
            }
        } catch (e: Exception) {
            // Delete of a missing pipeline is idempotent-success territory. OpenSearch throws
            // ResourceNotFoundException for missing pipeline; treat as Success.
            if (e is org.opensearch.ResourceNotFoundException ||
                e.cause is org.opensearch.ResourceNotFoundException
            ) {
                log.debug("ingest_pipelines: delete {} found nothing locally (ok)", name)
                return ApplyResult.Success
            }
            ApplyResult.TransientFailure("DeletePipeline failed for $name: ${e.message}", e)
        }
    }
}
