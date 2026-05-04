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

package org.opensearch.replication.action.switchover

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.waitForClusterStateUpdate
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException

class TransportFenceIndexAction @Inject constructor(
    transportService: TransportService,
    clusterService: ClusterService,
    threadPool: ThreadPool,
    actionFilters: ActionFilters,
    indexNameExpressionResolver: IndexNameExpressionResolver
) : TransportClusterManagerNodeAction<FenceIndexRequest, AcknowledgedResponse>(
    FenceIndexAction.NAME, transportService, clusterService, threadPool, actionFilters,
    ::FenceIndexRequest, indexNameExpressionResolver
), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportFenceIndexAction::class.java)
    }

    override fun checkBlock(request: FenceIndexRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    @Throws(Exception::class)
    override fun clusterManagerOperation(
        request: FenceIndexRequest,
        state: ClusterState,
        listener: ActionListener<AcknowledgedResponse>
    ) {
        log.info("Applying switchover fence update ${request.updateType} on ${request.indexName}")
        launch(threadPool.coroutineContext(ThreadPool.Names.MANAGEMENT)) {
            listener.completeWith { applyFenceUpdate(request) }
        }
    }

    private suspend fun applyFenceUpdate(request: FenceIndexRequest): AcknowledgedResponse {
        val response: AcknowledgedResponse = clusterService.waitForClusterStateUpdate("switchover-fence-${request.updateType}") { l ->
            FenceIndexTask(request, l)
        }
        if (!response.isAcknowledged) {
            throw OpenSearchException("Failed to apply switchover fence ${request.updateType} to index:${request.indexName}")
        }
        return response
    }

    override fun executor(): String = ThreadPool.Names.SAME

    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse = AcknowledgedResponse(inp)
}
