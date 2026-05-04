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
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchException
import org.opensearch.action.admin.indices.flush.FlushRequest
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.single.shard.TransportSingleShardAction
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.routing.ShardsIterator
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.action.ActionListener
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.Writeable
import org.opensearch.core.index.shard.ShardId
import org.opensearch.indices.IndicesService
import org.opensearch.replication.ReplicationPlugin.Companion.REPLICATION_EXECUTOR_NAME_LEADER
import org.opensearch.replication.util.completeWith
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.waitForGlobalCheckpoint
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService

/**
 * Flush a primary shard of a fenced index and return its durable handoff checkpoint.
 *
 * Preconditions:
 *   - The index must already carry INDEX_SWITCHOVER_FENCE_BLOCK. Computing a handoff
 *     checkpoint on a shard that still accepts writes would produce a moving value, so
 *     we refuse to proceed if the fence is not present.
 *
 * Behavior:
 *   1. Route to the primary (replicas do not own the local checkpoint advance).
 *   2. Wait for the in-cluster global checkpoint to catch up to the primary's local
 *      checkpoint. This is the moment quiescence is achieved within the local cluster:
 *      every in-sync copy has durably received and applied operations up to the returned
 *      seqno, so an in-cluster primary failover cannot rewind below that point.
 *   3. Flush, so the Lucene commit captures everything through the handoff seqno and the
 *      translog can be trimmed.
 *   4. Return the handoff seqno.
 *
 * Idempotency: invoking on a shard that has already been quiesced simply re-observes the
 * same global checkpoint (no new ops can have landed — the fence is in place) and the
 * flush is a no-op if nothing has changed.
 */
class TransportFlushAndGetHandoffCheckpointShardAction @Inject constructor(
    threadPool: ThreadPool,
    clusterService: ClusterService,
    transportService: TransportService,
    actionFilters: ActionFilters,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    private val indicesService: IndicesService
) : TransportSingleShardAction<FlushAndGetHandoffCheckpointShardRequest, FlushAndGetHandoffCheckpointShardResponse>(
    FlushAndGetHandoffCheckpointShardAction.NAME, threadPool, clusterService, transportService, actionFilters,
    indexNameExpressionResolver, ::FlushAndGetHandoffCheckpointShardRequest, REPLICATION_EXECUTOR_NAME_LEADER
) {

    companion object {
        private val log = LogManager.getLogger(TransportFlushAndGetHandoffCheckpointShardAction::class.java)
        private val WAIT_FOR_GCP_TIMEOUT = TimeValue.timeValueMinutes(1)
    }

    override fun shardOperation(
        request: FlushAndGetHandoffCheckpointShardRequest,
        shardId: ShardId
    ): FlushAndGetHandoffCheckpointShardResponse {
        throw UnsupportedOperationException("use asyncShardOperation")
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    override fun asyncShardOperation(
        request: FlushAndGetHandoffCheckpointShardRequest,
        shardId: ShardId,
        listener: ActionListener<FlushAndGetHandoffCheckpointShardResponse>
    ) {
        GlobalScope.launch(threadPool.coroutineContext(REPLICATION_EXECUTOR_NAME_LEADER)) {
            listener.completeWith {
                val state = clusterService.state()
                if (!state.blocks().hasIndexBlock(shardId.indexName, INDEX_SWITCHOVER_FENCE_BLOCK)) {
                    throw OpenSearchException(
                        "Refusing to compute handoff checkpoint for shard ${shardId}: " +
                            "switchover fence block is not installed on index ${shardId.indexName}"
                    )
                }

                val indexShard = indicesService.indexServiceSafe(shardId.index).getShard(shardId.id)

                val localCheckpoint = indexShard.localCheckpoint
                val maxSeqNo = indexShard.seqNoStats().maxSeqNo
                log.info("Computing handoff checkpoint for shard $shardId: local checkpoint=$localCheckpoint, max seqno=$maxSeqNo")

                // Wait for global checkpoint to reach the local checkpoint. Because the fence
                // is already installed, no new ops can land, so the local checkpoint is stable
                // and the global checkpoint will monotonically advance to meet it.
                val gcp = if (localCheckpoint >= 0) {
                    indexShard.waitForGlobalCheckpoint(localCheckpoint, WAIT_FOR_GCP_TIMEOUT)
                } else {
                    localCheckpoint
                }

                // Flush so the Lucene commit captures state through the handoff seqno and
                // translog can be trimmed.
                indexShard.flush(FlushRequest().waitIfOngoing(true).force(false))

                log.info("Handoff checkpoint for shard $shardId: handoff seqno=$gcp, last synced gcp=${indexShard.lastSyncedGlobalCheckpoint}")
                FlushAndGetHandoffCheckpointShardResponse(
                    shardId = shardId,
                    handoffSeqNo = gcp,
                    maxSeqNo = maxSeqNo,
                    primary = true
                )
            }
        }
    }

    override fun resolveIndex(request: FlushAndGetHandoffCheckpointShardRequest): Boolean = true

    override fun getResponseReader(): Writeable.Reader<FlushAndGetHandoffCheckpointShardResponse> =
        Writeable.Reader { inp: StreamInput -> FlushAndGetHandoffCheckpointShardResponse(inp) }

    override fun shards(state: ClusterState, request: InternalRequest): ShardsIterator {
        // Route via the concrete index resolved by the TransportSingleShardAction
        // framework: the caller's request carries a ShardId built with a placeholder index
        // UUID, which would miss the strict UUID check inside
        // RoutingTable#shardRoutingTable. Using the concrete name lets RoutingTable
        // re-resolve to the real Index + UUID.
        return state.routingTable()
            .shardRoutingTable(request.concreteIndex(), request.request().shardId.id)
            .primaryShardIt()
    }
}
