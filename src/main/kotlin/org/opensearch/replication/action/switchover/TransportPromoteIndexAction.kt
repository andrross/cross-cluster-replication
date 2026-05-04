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

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.HandledTransportAction
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.tasks.TaskId
import org.opensearch.index.IndexNotFoundException
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.suspendExecute
import org.opensearch.tasks.Task
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import org.opensearch.transport.client.Client

/**
 * Promote a follower index to leader. Operator-facing.
 *
 * Sequence:
 *   1. Fan out shard-level promote to every primary shard. Each shard's engine is
 *      swapped in place from ReplicationEngine to InternalEngine via
 *      IndexShard.resetToWriteableEngine.
 *   2. Invoke [FinalizePromoteAction] (a cluster-manager action) to atomically remove
 *      INDEX_REPLICATION_BLOCK and REPLICATED_INDEX_SETTING. Removing the setting is
 *      essential: without it, a subsequent reverse-direction _start call fails because
 *      TransportReplicateIndexAction rejects "Cannot Replicate a Replicated Index".
 *
 * Not cleaned up here (intentional):
 *   - The ShardReplicationTask that was pulling into this shard when it was a follower
 *     remains running, polling the (now fenced) former leader. It will idle
 *     indefinitely. Benign but wasteful; cleaned up on the next explicit _stop or by
 *     future reconciler logic.
 *
 * Failure handling: if any shard promote fails, the whole call fails. Successfully-
 * promoted shards remain in the LEADER role in the registry; the block and setting are
 * not modified. The operator can retry (promote shard is idempotent) or take manual
 * corrective action.
 */
class TransportPromoteIndexAction @Inject constructor(
    transportService: TransportService,
    actionFilters: ActionFilters,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService,
    private val client: Client
) : HandledTransportAction<PromoteIndexRequest, PromoteIndexResponse>(
    PromoteIndexAction.NAME, transportService, actionFilters, ::PromoteIndexRequest
) {

    companion object {
        private val log = LogManager.getLogger(TransportPromoteIndexAction::class.java)
    }

    override fun doExecute(
        task: Task,
        request: PromoteIndexRequest,
        listener: ActionListener<PromoteIndexResponse>
    ) {
        GlobalScope.launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            listener.completeWith { run(task, request) }
        }
    }

    private suspend fun run(task: Task, request: PromoteIndexRequest): PromoteIndexResponse {
        val state = clusterService.state()
        val indexRouting = state.routingTable().index(request.indexName)
            ?: throw IndexNotFoundException(request.indexName)

        val indexShardIds = indexRouting.shards().keys
        val unknown = request.targetSeqNos.keys - indexShardIds
        if (unknown.isNotEmpty()) {
            throw OpenSearchException(
                "Unknown shard ids for index ${request.indexName}: $unknown (index has shards $indexShardIds)"
            )
        }

        val parentTaskId = TaskId(clusterService.localNode().id, task.id)
        log.info("Promoting index ${request.indexName} across ${request.targetSeqNos.size} primary shard(s)")

        val shardResponses = request.targetSeqNos
            .map { (shardIdInt, targetSeqNo) ->
                val shardId: ShardId = indexRouting.shard(shardIdInt).shardId
                val shardRequest = PromoteShardRequest(shardId, targetSeqNo).apply { parentTask = parentTaskId }
                GlobalScope.async(Dispatchers.Unconfined + threadPool.coroutineContext()) {
                    client.suspendExecute(PromoteShardAction.INSTANCE, shardRequest)
                }
            }
            .awaitAll()

        // Finalize in cluster state. Delegated to a cluster-manager action because this
        // action is a HandledTransportAction and may run on any node.
        val finalizeResp = client.suspendExecute(
            FinalizePromoteAction.INSTANCE,
            FinalizePromoteRequest(request.indexName),
            injectSecurityContext = true
        )
        if (!finalizeResp.isAcknowledged) {
            throw OpenSearchException("Failed to finalize promote for ${request.indexName}")
        }

        return PromoteIndexResponse(request.indexName, shardResponses)
    }
}
