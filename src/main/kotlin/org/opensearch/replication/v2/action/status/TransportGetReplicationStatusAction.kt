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

package org.opensearch.replication.v2.action.status

import org.opensearch.ResourceNotFoundException
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.replication.v2.MetadataReplicationController
import org.opensearch.replication.v2.ReplicationIntent
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService

/**
 * Cluster-manager-scoped read of the metadata replication controller's live state for a given
 * relationship. 404 (via ResourceNotFoundException) if no intent is configured under that
 * relationship_id.
 *
 * This is a read-only action; the elected cluster manager answers it directly from the
 * controller component and current cluster state.
 */
class TransportGetReplicationStatusAction @Inject constructor(
    transportService: TransportService,
    clusterService: ClusterService,
    threadPool: ThreadPool,
    actionFilters: ActionFilters,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    private val controller: MetadataReplicationController
) : TransportClusterManagerNodeReadAction<GetReplicationStatusRequest, GetReplicationStatusResponse>(
    GetReplicationStatusAction.NAME,
    transportService, clusterService, threadPool, actionFilters,
    { inp -> GetReplicationStatusRequest(inp) }, indexNameExpressionResolver
) {

    override fun checkBlock(request: GetReplicationStatusRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ)
    }

    override fun clusterManagerOperation(
        request: GetReplicationStatusRequest,
        state: ClusterState,
        listener: ActionListener<GetReplicationStatusResponse>
    ) {
        val relationshipId = request.relationshipId!!
        val intent = ReplicationIntent.Reader.from(state.metadata)
        if (intent == null || intent.relationshipId != relationshipId) {
            listener.onFailure(
                ResourceNotFoundException(
                    "no replication intent configured for relationship_id=[$relationshipId]"
                )
            )
            return
        }
        val stats = controller.stats()
        listener.onResponse(
            GetReplicationStatusResponse(
                relationshipId = intent.relationshipId,
                localAlias = intent.localAlias,
                remoteAlias = intent.remoteAlias,
                loopActive = stats.active,
                lastAppliedMetadataVersion = stats.lastAppliedMetadataVersion
            )
        )
    }

    override fun executor(): String = ThreadPool.Names.SAME

    override fun read(inp: StreamInput): GetReplicationStatusResponse = GetReplicationStatusResponse(inp)
}
