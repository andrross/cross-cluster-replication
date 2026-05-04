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

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchTimeoutException
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.single.shard.TransportSingleShardAction
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.routing.ShardsIterator
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.index.shard.ShardId
import org.opensearch.indices.IndicesService
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATION_EXECUTOR_NAME_FOLLOWER
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService

/**
 * Wait for a single follower primary shard's local checkpoint to reach a target seqno.
 *
 * This is the observation side of quiescence: given a handoff seqno computed on the leader
 * (see [TransportFlushAndGetHandoffCheckpointShardAction]), confirm the follower shard has
 * applied all operations up to and including that seqno.
 *
 * Behavior:
 *   1. Route to the primary of the follower shard (replicas don't independently advance
 *      the local checkpoint; the primary is the authoritative source for follower progress).
 *   2. Poll indexShard.localCheckpoint with small fixed-delay sleeps until it is >= target,
 *      or the timeout elapses.
 *   3. Return the observed checkpoint on success, throw OpenSearchTimeoutException on
 *      timeout. The response always carries the last observed checkpoint so callers can
 *      see how close they got.
 *
 * Why polling instead of a listener: OpenSearch core exposes a global-checkpoint listener
 * (addGlobalCheckpointListener) but no comparable local-checkpoint listener — local
 * checkpoint is managed inside InternalEngine's LocalCheckpointTracker with no public
 * subscribe API. Polling with a short interval is cheap, bounded, and doesn't require
 * reaching into engine internals.
 *
 * Idempotency: calling on a shard that's already caught up returns immediately on the
 * first poll observation.
 */
class TransportVerifyCaughtUpShardAction @Inject constructor(
    threadPool: ThreadPool,
    clusterService: ClusterService,
    transportService: TransportService,
    actionFilters: ActionFilters,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    private val indicesService: IndicesService
) : TransportSingleShardAction<VerifyCaughtUpShardRequest, VerifyCaughtUpShardResponse>(
    VerifyCaughtUpShardAction.NAME, threadPool, clusterService, transportService, actionFilters,
    indexNameExpressionResolver, ::VerifyCaughtUpShardRequest, REPLICATION_EXECUTOR_NAME_FOLLOWER
) {

    companion object {
        private val log = LogManager.getLogger(TransportVerifyCaughtUpShardAction::class.java)
        private const val POLL_INTERVAL_MILLIS = 50L
    }

    override fun shardOperation(
        request: VerifyCaughtUpShardRequest,
        shardId: ShardId
    ): VerifyCaughtUpShardResponse {
        throw UnsupportedOperationException("use asyncShardOperation")
    }

    override fun asyncShardOperation(
        request: VerifyCaughtUpShardRequest,
        shardId: ShardId,
        listener: ActionListener<VerifyCaughtUpShardResponse>
    ) {
        GlobalScope.launch(threadPool.coroutineContext(REPLICATION_EXECUTOR_NAME_FOLLOWER)) {
            listener.completeWith {
                val indexShard = indicesService.indexServiceSafe(shardId.index).getShard(shardId.id)
                val deadline = System.nanoTime() + request.timeoutMillis * 1_000_000L

                var localCheckpoint = indexShard.localCheckpoint
                while (localCheckpoint < request.targetSeqNo) {
                    if (System.nanoTime() >= deadline) {
                        log.info(
                            "VerifyCaughtUp timeout for shard $shardId: " +
                                "local=$localCheckpoint target=${request.targetSeqNo}"
                        )
                        throw OpenSearchTimeoutException(
                            "Timed out waiting for shard $shardId to catch up to seqno ${request.targetSeqNo}; " +
                                "last observed local checkpoint was $localCheckpoint"
                        )
                    }
                    delay(POLL_INTERVAL_MILLIS)
                    localCheckpoint = indexShard.localCheckpoint
                }

                log.info("VerifyCaughtUp shard $shardId caught up: local=$localCheckpoint target=${request.targetSeqNo}")
                VerifyCaughtUpShardResponse(
                    shardId = shardId,
                    targetSeqNo = request.targetSeqNo,
                    localCheckpoint = localCheckpoint
                )
            }
        }
    }

    override fun resolveIndex(request: VerifyCaughtUpShardRequest): Boolean = true

    override fun getResponseReader(): Writeable.Reader<VerifyCaughtUpShardResponse> =
        Writeable.Reader { inp: StreamInput -> VerifyCaughtUpShardResponse(inp) }

    override fun shards(state: ClusterState, request: InternalRequest): ShardsIterator {
        return state.routingTable()
            .shardRoutingTable(request.concreteIndex(), request.request().shardId.id)
            .primaryShardIt()
    }
}
