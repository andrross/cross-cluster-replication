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
 * Promote a single follower primary shard to leader. Internal primitive; operators call
 * the index-level PromoteIndex action which fans out to every shard.
 *
 * Preconditions (caller's responsibility):
 *   - The shard has caught up to `targetSeqNo` (verified via VerifyCaughtUpAction).
 *   - The leader-side index is fenced and will not produce further ops.
 *
 * What this action does:
 *   1. Verify the shard's local checkpoint has reached `targetSeqNo`. This is a safety
 *      re-check — the caller should have already verified via the cluster-scoped
 *      VerifyCaughtUp, but racing this against stale assumptions would be a correctness
 *      bug, so we double-check at the shard level.
 *   2. Set the plugin's per-shard role override to LEADER. The engine factory will
 *      consult this on the next engine construction.
 *   3. Call indexShard.resetToWriteableEngine(). This blocks operations, flushes the
 *      current engine, builds a new one via the factory (which now returns
 *      InternalEngine), replays the translog, and atomically swaps. After this returns,
 *      the shard admits writes through the normal primary path.
 *
 * Not this action's responsibility:
 *   - Removing the INDEX_REPLICATION_BLOCK on the follower index. That's a cluster-state
 *     operation and belongs at the index level.
 *   - Stopping the ShardReplicationTask. The pull task will find the leader fenced and
 *     idle out; the index-level caller can clean it up as a separate step.
 *
 * Idempotency: if the shard is already operating as LEADER (role override already set),
 * this action succeeds without doing work. resetToWriteableEngine is safe to call even
 * when the engine is already write-capable, though it is expensive.
 */
class TransportPromoteShardAction @Inject constructor(
    threadPool: ThreadPool,
    clusterService: ClusterService,
    transportService: TransportService,
    actionFilters: ActionFilters,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    private val indicesService: IndicesService,
    private val switchoverRoleRegistry: SwitchoverRoleRegistry
) : TransportSingleShardAction<PromoteShardRequest, PromoteShardResponse>(
    PromoteShardAction.NAME, threadPool, clusterService, transportService, actionFilters,
    indexNameExpressionResolver, ::PromoteShardRequest, REPLICATION_EXECUTOR_NAME_FOLLOWER
) {

    companion object {
        private val log = LogManager.getLogger(TransportPromoteShardAction::class.java)
    }

    override fun shardOperation(request: PromoteShardRequest, shardId: ShardId): PromoteShardResponse {
        throw UnsupportedOperationException("use asyncShardOperation")
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    override fun asyncShardOperation(
        request: PromoteShardRequest,
        shardId: ShardId,
        listener: ActionListener<PromoteShardResponse>
    ) {
        GlobalScope.launch(threadPool.coroutineContext(REPLICATION_EXECUTOR_NAME_FOLLOWER)) {
            listener.completeWith {
                val indexShard = indicesService.indexServiceSafe(shardId.index).getShard(shardId.id)

                val localCheckpoint = indexShard.localCheckpoint
                if (localCheckpoint < request.targetSeqNo) {
                    throw OpenSearchException(
                        "Refusing to promote shard $shardId: local checkpoint $localCheckpoint " +
                            "is below target seqno ${request.targetSeqNo}"
                    )
                }

                switchoverRoleRegistry.set(shardId, SwitchoverRole.LEADER)
                log.info("Promoting shard $shardId at local checkpoint $localCheckpoint; resetting engine")

                // Block ops, flush, build new engine via factory (now returns InternalEngine),
                // replay translog, atomic swap. This is OpenSearch's existing primitive for
                // live engine replacement — the same one used by segment-replication
                // primary promotion and relocation handoff.
                indexShard.resetToWriteableEngine()

                log.info("Shard $shardId promoted to LEADER; engine reset complete")
                PromoteShardResponse(shardId = shardId, localCheckpoint = indexShard.localCheckpoint)
            }
        }
    }

    override fun resolveIndex(request: PromoteShardRequest): Boolean = true

    override fun getResponseReader(): Writeable.Reader<PromoteShardResponse> =
        Writeable.Reader { inp: StreamInput -> PromoteShardResponse(inp) }

    override fun shards(state: ClusterState, request: InternalRequest): ShardsIterator {
        return state.routingTable()
            .shardRoutingTable(request.concreteIndex(), request.request().shardId.id)
            .primaryShardIt()
    }
}
