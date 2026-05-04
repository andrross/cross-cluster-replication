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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.action.admin.indices.close.CloseIndexRequest
import org.opensearch.action.admin.indices.open.OpenIndexRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction
import org.opensearch.cluster.AckedClusterStateUpdateTask
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.block.ClusterBlocks
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.settings.Settings
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import org.opensearch.replication.metadata.INDEX_REPLICATION_BLOCK
import org.opensearch.replication.metadata.UpdateMetadataAction
import org.opensearch.replication.metadata.UpdateMetadataRequest
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.waitForClusterStateUpdate
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

/**
 * Demote an index from leader to follower shape.
 *
 * Precondition: the index must already carry INDEX_SWITCHOVER_FENCE_BLOCK — i.e., _fence
 * was called and the leader is no longer admitting writes. Demoting without a prior fence
 * would be a correctness bug: we'd be installing a follower-shape engine on an index that
 * could still be receiving writes.
 *
 * Sequence:
 *   1. Close the index. This lets us change engine-affecting settings cleanly.
 *   2. Add REPLICATED_INDEX_SETTING to the index settings. On reopen, the plugin's
 *      getEngineFactory sees this setting and installs its factory, which returns
 *      ReplicationEngine.
 *   3. Install INDEX_REPLICATION_BLOCK (the standard CCR follower write block).
 *   4. Remove INDEX_SWITCHOVER_FENCE_BLOCK (no longer needed; the CCR block subsumes it).
 *   5. Open the index. The shard starts up with ReplicationEngine installed.
 *
 * After demote: the index is in follower shape but not yet actively replicating. The
 * operator (or future reconciler) must call _start to establish the pull from the peer
 * leader. This action does not attempt to start replication — starting is a separate
 * concern and requires knowing the peer cluster's connection alias, which varies.
 *
 * Idempotency: calling demote on an already-demoted index is safe. The fence block check
 * fails first (there's no fence on an already-follower index), so the second call errors
 * out cleanly rather than re-running the steps.
 */
class TransportDemoteIndexAction @Inject constructor(
    transportService: TransportService,
    clusterService: ClusterService,
    threadPool: ThreadPool,
    actionFilters: ActionFilters,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    val client: Client
) : TransportClusterManagerNodeAction<DemoteIndexRequest, AcknowledgedResponse>(
    DemoteIndexAction.NAME, transportService, clusterService, threadPool, actionFilters,
    ::DemoteIndexRequest, indexNameExpressionResolver
), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportDemoteIndexAction::class.java)
    }

    override fun checkBlock(request: DemoteIndexRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    override fun clusterManagerOperation(
        request: DemoteIndexRequest,
        state: ClusterState,
        listener: ActionListener<AcknowledgedResponse>
    ) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            try {
                run(request)
                listener.onResponse(AcknowledgedResponse(true))
            } catch (e: Exception) {
                log.error("Demote failed for index ${request.indexName}", e)
                listener.onFailure(e)
            }
        }
    }

    private suspend fun run(request: DemoteIndexRequest) {
        val currentState = clusterService.state()
        if (!currentState.blocks().hasIndexBlock(request.indexName, INDEX_SWITCHOVER_FENCE_BLOCK)) {
            throw OpenSearchException(
                "Refusing to demote ${request.indexName}: switchover fence block is not installed " +
                    "(fence the index first via _fence)"
            )
        }

        log.info("Demoting index ${request.indexName} (will follow ${request.leaderIndexName})")

        // 1. Close the index so we can change engine-affecting settings. Closing under the
        // active fence block is allowed — the fence only rejects writes and metadata
        // writes, not index close (which is a cluster-state-scope metadata operation).
        val closeResp = client.suspendExecute(
            UpdateMetadataAction.INSTANCE,
            UpdateMetadataRequest(
                request.indexName,
                UpdateMetadataRequest.Type.CLOSE,
                CloseIndexRequest(request.indexName)
            ),
            injectSecurityContext = true
        )
        if (!closeResp.isAcknowledged) {
            throw OpenSearchException("Failed to close ${request.indexName} during demote")
        }

        // 2. One atomic cluster-state update that: adds REPLICATED_INDEX_SETTING to the
        // index metadata (bypassing the InternalIndex protection that UpdateSettings
        // enforces), installs INDEX_REPLICATION_BLOCK, and removes INDEX_SWITCHOVER_
        // FENCE_BLOCK. Doing all three in one task eliminates any intermediate state
        // where, say, the fence is lifted but the CCR block isn't yet in place.
        val applyResp: AcknowledgedResponse = clusterService.waitForClusterStateUpdate("demote-${request.indexName}") { l ->
            DemoteClusterStateTask(request, l)
        }
        if (!applyResp.isAcknowledged) {
            throw OpenSearchException("Failed to apply demote cluster state update for ${request.indexName}")
        }

        // 3. Reopen. The shard comes up with ReplicationEngine installed because the
        // plugin's engine factory now sees REPLICATED_INDEX_SETTING.
        val openResp = client.suspendExecute(
            UpdateMetadataAction.INSTANCE,
            UpdateMetadataRequest(
                request.indexName,
                UpdateMetadataRequest.Type.OPEN,
                OpenIndexRequest(request.indexName)
            ),
            injectSecurityContext = true
        )
        if (!openResp.isAcknowledged) {
            throw OpenSearchException("Failed to reopen ${request.indexName} during demote")
        }

        log.info("Demoted ${request.indexName} successfully")
    }

    /**
     * Single atomic cluster-state update that transforms the index from leader-side into
     * follower-side shape: adds REPLICATED_INDEX_SETTING, installs INDEX_REPLICATION_BLOCK,
     * removes INDEX_SWITCHOVER_FENCE_BLOCK.
     *
     * Bypasses UpdateSettings' InternalIndex protection by mutating IndexMetadata directly,
     * mirroring how StopReplicationTask removes the setting on stop.
     */
    class DemoteClusterStateTask(
        val request: DemoteIndexRequest,
        listener: ActionListener<AcknowledgedResponse>
    ) : AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

        override fun execute(currentState: ClusterState): ClusterState {
            val builder = ClusterState.builder(currentState)

            // Add REPLICATED_INDEX_SETTING to the index metadata.
            val currentMetadata = currentState.metadata.index(request.indexName)
                ?: throw OpenSearchException("Index ${request.indexName} disappeared during demote")
            val newIndexMetadata = IndexMetadata.builder(currentMetadata)
                .settings(
                    Settings.builder()
                        .put(currentMetadata.settings)
                        .put(REPLICATED_INDEX_SETTING.key, request.leaderIndexName)
                )
                .settingsVersion(1 + currentMetadata.settingsVersion)
            builder.metadata(Metadata.builder(currentState.metadata).put(newIndexMetadata))

            // Swap the blocks: add CCR write block, remove switchover fence block.
            val newBlocks = ClusterBlocks.builder().blocks(currentState.blocks)
            if (!currentState.blocks.hasIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)) {
                newBlocks.addIndexBlock(request.indexName, INDEX_REPLICATION_BLOCK)
            }
            newBlocks.removeIndexBlock(request.indexName, INDEX_SWITCHOVER_FENCE_BLOCK)
            builder.blocks(newBlocks)

            return builder.build()
        }

        override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
    }

    override fun executor(): String = ThreadPool.Names.SAME

    override fun read(inp: StreamInput): AcknowledgedResponse = AcknowledgedResponse(inp)
}
