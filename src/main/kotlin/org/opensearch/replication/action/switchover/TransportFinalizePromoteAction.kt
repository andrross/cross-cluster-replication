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
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.persistent.RemovePersistentTaskAction
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATED_INDEX_SETTING
import org.opensearch.replication.metadata.INDEX_REPLICATION_BLOCK
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.waitForClusterStateUpdate
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

/**
 * Cluster-manager action that finalizes promote: removes INDEX_REPLICATION_BLOCK and
 * REPLICATED_INDEX_SETTING in one atomic cluster-state update. Bypasses
 * UpdateSettings's InternalIndex protection by mutating IndexMetadata directly —
 * same pattern StopReplicationTask already uses to remove the setting.
 */
class TransportFinalizePromoteAction @Inject constructor(
    transportService: TransportService,
    clusterService: ClusterService,
    threadPool: ThreadPool,
    actionFilters: ActionFilters,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    val client: Client
) : TransportClusterManagerNodeAction<FinalizePromoteRequest, AcknowledgedResponse>(
    FinalizePromoteAction.NAME, transportService, clusterService, threadPool, actionFilters,
    ::FinalizePromoteRequest, indexNameExpressionResolver
), CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportFinalizePromoteAction::class.java)
    }

    override fun checkBlock(request: FinalizePromoteRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }

    override fun clusterManagerOperation(
        request: FinalizePromoteRequest,
        state: ClusterState,
        listener: ActionListener<AcknowledgedResponse>
    ) {
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            try {
                // Cancel any leftover CCR persistent tasks for this index. The index-level
                // task and shard-level tasks were running when this index was a follower.
                // After promote they would idle against a fenced former leader; worse,
                // starting a fresh _start in the reverse direction collides on the
                // persistent-task id. Remove them now.
                cancelReplicationPersistentTasks(request.indexName)

                val resp: AcknowledgedResponse = clusterService.waitForClusterStateUpdate("finalize-promote-${request.indexName}") { l ->
                    FinalizePromoteTask(request.indexName, l)
                }
                if (!resp.isAcknowledged) {
                    throw OpenSearchException("Failed to finalize promote for ${request.indexName}")
                }
                listener.onResponse(AcknowledgedResponse(true))
            } catch (e: Exception) {
                log.error("Failed to finalize promote for ${request.indexName}", e)
                listener.onFailure(e)
            }
        }
    }

    /**
     * Remove replication persistent tasks for an index. Mirrors the cleanup
     * `TransportStopIndexReplicationAction.removeStaleReplicationTasksFromClusterState`
     * performs — same id convention: "replication:index:<index>" for the index-level
     * task and "replication:[<index>][<shard>]" for shard-level tasks.
     */
    private suspend fun cancelReplicationPersistentTasks(indexName: String) {
        val allTasks: PersistentTasksCustomMetadata? =
            clusterService.state().metadata().custom(PersistentTasksCustomMetadata.TYPE)
        allTasks?.tasks()?.forEach { task ->
            val taskId = task.id
            val matchesIndex = taskId.startsWith("replication:") && (
                taskId == "replication:index:$indexName" ||
                    taskId.split(":").getOrNull(1)?.contains(indexName) == true
                )
            if (matchesIndex) {
                try {
                    log.info("Removing CCR persistent task $taskId during promote finalize")
                    client.suspendExecute(
                        RemovePersistentTaskAction.INSTANCE,
                        RemovePersistentTaskAction.Request(taskId)
                    )
                } catch (e: Exception) {
                    log.warn("Failed to remove CCR persistent task $taskId: ${e.message}")
                }
            }
        }
    }

    override fun executor(): String = ThreadPool.Names.SAME

    override fun read(inp: StreamInput): AcknowledgedResponse = AcknowledgedResponse(inp)

    class FinalizePromoteTask(
        val indexName: String,
        listener: ActionListener<AcknowledgedResponse>
    ) : AckedClusterStateUpdateTask<AcknowledgedResponse>(
        object : org.opensearch.cluster.ack.AckedRequest {
            override fun ackTimeout() = org.opensearch.common.unit.TimeValue.timeValueSeconds(30)
            override fun clusterManagerNodeTimeout() = org.opensearch.common.unit.TimeValue.timeValueSeconds(30)
        },
        listener
    ) {
        override fun execute(currentState: ClusterState): ClusterState {
            val builder = ClusterState.builder(currentState)

            val currentMetadata = currentState.metadata.index(indexName)
                ?: throw OpenSearchException("Index $indexName disappeared during promote finalize")
            if (currentMetadata.settings[REPLICATED_INDEX_SETTING.key] != null) {
                val newIndexMetadata = IndexMetadata.builder(currentMetadata)
                    .settings(
                        Settings.builder()
                            .put(currentMetadata.settings)
                            .putNull(REPLICATED_INDEX_SETTING.key)
                    )
                    .settingsVersion(1 + currentMetadata.settingsVersion)
                builder.metadata(Metadata.builder(currentState.metadata).put(newIndexMetadata))
            }

            if (currentState.blocks.hasIndexBlock(indexName, INDEX_REPLICATION_BLOCK)) {
                val newBlocks = ClusterBlocks.builder().blocks(currentState.blocks)
                    .removeIndexBlock(indexName, INDEX_REPLICATION_BLOCK)
                builder.blocks(newBlocks)
            }

            return builder.build()
        }

        override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
    }
}
