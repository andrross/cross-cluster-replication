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
 * Flush every primary shard of a fenced index and return the durable handoff checkpoint
 * for each shard, in parallel.
 *
 * This is the operator-facing granularity: a switchover involves fencing the index and
 * then taking its handoff checkpoints, both at the index level. Per-shard handling is an
 * internal transport primitive dispatched by this action — flushing is inherently
 * co-located with the primary shard's node, but operators do not need to think about
 * shards.
 *
 * Fan-out policy: all shards in parallel. If any shard fails, the whole call fails. The
 * fence remains installed — the operator's recovery path is abort (lift the fence) or
 * retry. Partial results are not a valid state for switchover.
 */
class TransportFlushAndGetHandoffCheckpointAction @Inject constructor(
    transportService: TransportService,
    actionFilters: ActionFilters,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService,
    private val client: Client
) : HandledTransportAction<FlushAndGetHandoffCheckpointRequest, FlushAndGetHandoffCheckpointResponse>(
    FlushAndGetHandoffCheckpointAction.NAME, transportService, actionFilters, ::FlushAndGetHandoffCheckpointRequest
) {

    companion object {
        private val log = LogManager.getLogger(TransportFlushAndGetHandoffCheckpointAction::class.java)
    }

    override fun doExecute(
        task: Task,
        request: FlushAndGetHandoffCheckpointRequest,
        listener: ActionListener<FlushAndGetHandoffCheckpointResponse>
    ) {
        GlobalScope.launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            listener.completeWith { run(task, request) }
        }
    }

    private suspend fun run(
        task: Task,
        request: FlushAndGetHandoffCheckpointRequest
    ): FlushAndGetHandoffCheckpointResponse {
        val state = clusterService.state()

        if (!state.blocks().hasIndexBlock(request.indexName, INDEX_SWITCHOVER_FENCE_BLOCK)) {
            throw OpenSearchException(
                "Refusing to compute handoff checkpoint for index ${request.indexName}: " +
                    "switchover fence block is not installed"
            )
        }

        val indexRouting = state.routingTable().index(request.indexName)
            ?: throw IndexNotFoundException(request.indexName)

        val parentTaskId = TaskId(clusterService.localNode().id, task.id)
        val shardIds: List<ShardId> = indexRouting.shards().keys.sorted().map { indexRouting.shard(it).shardId }
        log.info("Computing handoff checkpoints for index ${request.indexName} across ${shardIds.size} primary shard(s)")

        val shardResponses = shardIds
            .map { shardId ->
                val shardRequest = FlushAndGetHandoffCheckpointShardRequest(shardId).apply { parentTask = parentTaskId }
                GlobalScope.async(Dispatchers.Unconfined + threadPool.coroutineContext()) {
                    client.suspendExecute(FlushAndGetHandoffCheckpointShardAction.INSTANCE, shardRequest)
                }
            }
            .awaitAll()

        return FlushAndGetHandoffCheckpointResponse(request.indexName, shardResponses)
    }
}
