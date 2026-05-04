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
 * Wait until every primary shard of the follower index has applied ops through the given
 * per-shard target seqnos. This is the observation side of quiescence: the leader has
 * already produced handoff seqnos via [TransportFlushAndGetHandoffCheckpointAction]; this
 * action confirms the follower has caught up to them.
 *
 * Fan-out policy: all shards in parallel, each with the same timeout. If any shard times
 * out or errors, the whole call fails — partial success is not a valid state of the world
 * for switchover (the role swap relies on every shard being at equality).
 */
class TransportVerifyCaughtUpAction @Inject constructor(
    transportService: TransportService,
    actionFilters: ActionFilters,
    private val threadPool: ThreadPool,
    private val clusterService: ClusterService,
    private val client: Client
) : HandledTransportAction<VerifyCaughtUpRequest, VerifyCaughtUpResponse>(
    VerifyCaughtUpAction.NAME, transportService, actionFilters, ::VerifyCaughtUpRequest
) {

    companion object {
        private val log = LogManager.getLogger(TransportVerifyCaughtUpAction::class.java)
    }

    override fun doExecute(
        task: Task,
        request: VerifyCaughtUpRequest,
        listener: ActionListener<VerifyCaughtUpResponse>
    ) {
        GlobalScope.launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            listener.completeWith { run(task, request) }
        }
    }

    private suspend fun run(task: Task, request: VerifyCaughtUpRequest): VerifyCaughtUpResponse {
        val state = clusterService.state()
        val indexRouting = state.routingTable().index(request.indexName)
            ?: throw IndexNotFoundException(request.indexName)

        // Validate that every shard we've been asked about actually exists. Catching a
        // typo'd shard id here is far more debuggable than having the shard-level action
        // time out on a shard that never advances because nothing ever routes to it.
        val indexShardIds: Set<Int> = indexRouting.shards().keys
        val unknownShards = request.targetSeqNos.keys - indexShardIds
        if (unknownShards.isNotEmpty()) {
            throw OpenSearchException(
                "Unknown shard ids for index ${request.indexName}: $unknownShards (index has shards $indexShardIds)"
            )
        }

        val parentTaskId = TaskId(clusterService.localNode().id, task.id)
        log.info(
            "VerifyCaughtUp on index ${request.indexName} across " +
                "${request.targetSeqNos.size} shard(s), timeout=${request.timeoutMillis}ms"
        )

        val shardResponses = request.targetSeqNos
            .map { (shardIdInt, targetSeqNo) ->
                val shardId: ShardId = indexRouting.shard(shardIdInt).shardId
                val shardRequest = VerifyCaughtUpShardRequest(shardId, targetSeqNo, request.timeoutMillis)
                    .apply { parentTask = parentTaskId }
                GlobalScope.async(Dispatchers.Unconfined + threadPool.coroutineContext()) {
                    client.suspendExecute(VerifyCaughtUpShardAction.INSTANCE, shardRequest)
                }
            }
            .awaitAll()

        return VerifyCaughtUpResponse(request.indexName, shardResponses)
    }
}
