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

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.OpenSearchStatusException
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
import org.opensearch.core.rest.RestStatus
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.v2.BootstrapOrchestrator
import org.opensearch.replication.v2.ReplicationIntent
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client
import java.io.IOException

class TransportPutReplicationIntentAction @Inject constructor(
    transportService: TransportService,
    clusterService: ClusterService,
    threadPool: ThreadPool,
    actionFilters: ActionFilters,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    private val client: Client,
    private val bootstrap: BootstrapOrchestrator
) : TransportClusterManagerNodeAction<PutReplicationIntentRequest, AcknowledgedResponse>(
    PutReplicationIntentAction.NAME, transportService, clusterService, threadPool, actionFilters,
    { inp -> PutReplicationIntentRequest(inp) }, indexNameExpressionResolver
) {

    companion object {
        private val log = LogManager.getLogger(TransportPutReplicationIntentAction::class.java)
    }

    private val scope = CoroutineScope(
        SupervisorJob() + Dispatchers.Default + CoroutineName("put-replication-intent")
    )

    override fun checkBlock(request: PutReplicationIntentRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    override fun clusterManagerOperation(
        request: PutReplicationIntentRequest,
        state: ClusterState,
        listener: ActionListener<AcknowledgedResponse>
    ) {
        log.info("put-intent: clear={}, relationship_id={}, role={}",
            request.clear, request.relationshipId, request.role)

        val currentIntent = ReplicationIntent.Reader.from(state.metadata)

        // Idempotency guard: an existing intent under the same relationship_id with different
        // local/remote aliases or a different role is a conflict. Same aliases + same role is
        // a no-op re-apply (return 200 with current state).
        if (!request.clear && currentIntent != null && currentIntent.relationshipId == request.relationshipId) {
            val sameShape = currentIntent.role == request.role &&
                currentIntent.localAlias == request.localAlias &&
                currentIntent.remoteAlias == request.remoteAlias
            if (!sameShape) {
                listener.onFailure(OpenSearchStatusException(
                    "intent already exists under relationship_id=[${request.relationshipId}] " +
                        "with different shape (current=${currentIntent.role}/${currentIntent.localAlias}/" +
                        "${currentIntent.remoteAlias})",
                    RestStatus.CONFLICT
                ))
                return
            }
        }

        // DELETE (clear=true) on a SECONDARY-side intent must fully sever the follower
        // relationship (close + strip follower marker + reopen) BEFORE returning. Otherwise
        // the client sees a window where the intent is gone but the indices are still
        // using ReplicationEngine, and a client write into that window causes a fatal
        // assertion. Sever first, then clear the intent.
        if (request.clear && currentIntent?.isSecondary == true) {
            scope.launch {
                try {
                    bootstrap.severFollowerRelationship()
                    clusterService.submitStateUpdateTask(
                        "put-replication-intent",
                        PutIntentTask(request, listener)
                    )
                } catch (e: Exception) {
                    listener.onFailure(e)
                }
            }
            return
        }

        // PUT (clear=false) of a SECONDARY intent: auto-install the matching PRIMARY intent
        // on the peer cluster via the same transport action, then install locally. Peers
        // that already have a matching intent no-op. This keeps the two sides symmetric
        // without making the caller issue two PUTs.
        //
        // If `cluster.remote.<remoteAlias>.seeds` is not configured, `getRemoteClusterClient`
        // will throw; we propagate that error to the caller as-is.
        if (!request.clear && request.role == ReplicationIntent.Role.SECONDARY) {
            val remoteAlias = request.remoteAlias!!
            scope.launch {
                try {
                    installPrimaryOnPeer(request)
                    clusterService.submitStateUpdateTask(
                        "put-replication-intent",
                        PutIntentTask(request, listener)
                    )
                } catch (e: Exception) {
                    log.warn("auto-install PRIMARY on peer {} failed: {}", remoteAlias, e.message)
                    listener.onFailure(e)
                }
            }
            return
        }

        clusterService.submitStateUpdateTask(
            "put-replication-intent",
            PutIntentTask(request, listener)
        )
    }

    /**
     * Build the peer's intent (PRIMARY role, aliases swapped) and write it via an internal
     * transport hop to the peer cluster. Same action, so the same idempotency / conflict
     * checks run on the peer side.
     */
    private suspend fun installPrimaryOnPeer(secondaryRequest: PutReplicationIntentRequest) {
        val primaryRequest = PutReplicationIntentRequest().also {
            it.relationshipId = secondaryRequest.relationshipId
            it.role = ReplicationIntent.Role.PRIMARY
            // Labels are identity, not directional: the peer's "local" is our "remote" and
            // vice versa.
            it.localAlias = secondaryRequest.remoteAlias
            it.remoteAlias = secondaryRequest.localAlias
            it.epoch = secondaryRequest.epoch
            it.status = secondaryRequest.status
        }
        val remoteClient = client.getRemoteClusterClient(secondaryRequest.remoteAlias!!)
        remoteClient.suspendExecute(
            action = PutReplicationIntentAction.INSTANCE,
            req = primaryRequest,
            injectSecurityContext = true
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
                    relationshipId = request.relationshipId!!,
                    localAlias = request.localAlias!!,
                    remoteAlias = request.remoteAlias!!,
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
