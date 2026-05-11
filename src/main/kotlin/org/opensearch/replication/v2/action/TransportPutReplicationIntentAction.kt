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

package org.opensearch.replication.v2.action

import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction
import org.opensearch.cluster.AckedClusterStateUpdateTask
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.replication.v2.ReplicationIntent
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException

class TransportPutReplicationIntentAction @Inject constructor(
    transportService: TransportService,
    clusterService: ClusterService,
    threadPool: ThreadPool,
    actionFilters: ActionFilters,
    indexNameExpressionResolver: IndexNameExpressionResolver
) : TransportClusterManagerNodeAction<PutReplicationIntentRequest, AcknowledgedResponse>(
    PutReplicationIntentAction.NAME, transportService, clusterService, threadPool, actionFilters,
    { inp -> PutReplicationIntentRequest(inp) }, indexNameExpressionResolver
) {

    companion object {
        private val log = LogManager.getLogger(TransportPutReplicationIntentAction::class.java)
    }

    override fun checkBlock(request: PutReplicationIntentRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    override fun clusterManagerOperation(
        request: PutReplicationIntentRequest,
        state: ClusterState,
        listener: ActionListener<AcknowledgedResponse>
    ) {
        log.info("put-intent: clear={}, peer={}, role={}",
            request.clear, request.peerClusterAlias, request.role)
        clusterService.submitStateUpdateTask(
            "put-replication-intent",
            PutIntentTask(request, listener)
        )
    }

    private class PutIntentTask(
        val request: PutReplicationIntentRequest,
        listener: ActionListener<AcknowledgedResponse>
    ) : AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

        override fun execute(currentState: ClusterState): ClusterState {
            val metadataBuilder = Metadata.builder(currentState.metadata)
            if (request.clear) {
                metadataBuilder.removeCustom(ReplicationIntent.NAME)
            } else {
                val intent = ReplicationIntent(
                    peerClusterAlias = request.peerClusterAlias!!,
                    role = request.role!!,
                    epoch = request.epoch,
                    status = request.status
                )
                metadataBuilder.putCustom(ReplicationIntent.NAME, intent)
            }
            return ClusterState.builder(currentState).metadata(metadataBuilder).build()
        }

        override fun newResponse(acknowledged: Boolean): AcknowledgedResponse {
            if (!acknowledged) {
                throw OpenSearchException("put-intent update was not acknowledged")
            }
            return AcknowledgedResponse(true)
        }
    }

    override fun executor(): String = ThreadPool.Names.SAME

    @Throws(IOException::class)
    override fun read(inp: StreamInput): AcknowledgedResponse = AcknowledgedResponse(inp)
}
