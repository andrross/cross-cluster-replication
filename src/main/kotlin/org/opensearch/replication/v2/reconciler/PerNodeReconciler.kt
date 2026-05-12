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

package org.opensearch.replication.v2.reconciler

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.lifecycle.AbstractLifecycleComponent
import org.opensearch.common.unit.TimeValue
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.replication.v2.ReplicationIntent
import org.opensearch.replication.v2.ReplicationScope
import org.opensearch.replication.v2.shard.ShardWorker
import org.opensearch.threadpool.Scheduler
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.client.Client
import java.util.concurrent.ConcurrentHashMap

/**
 * Per-data-node reconciler. On every cluster-state change:
 *
 *   1. Read the replication intent and the local routing table.
 *   2. Compute which primary shards of replicated indices are hosted on this node.
 *   3. Start in-memory ShardWorkers for those shards if they aren't already running.
 *   4. Stop workers for shards this node no longer hosts, or that are no longer replicated.
 *
 * No persistent-tasks entries, no cluster-state updates emitted from the reconciler. Worker
 * lifecycle is in-process. If this node restarts, another node's reconciler will pick up the
 * shards it had, and so on.
 *
 * The reconciler only runs on the secondary (role=SECONDARY). On the primary side it is a
 * no-op: the replication loop pulls from the primary, nothing to reconcile locally.
 */
class PerNodeReconciler(
    private val clusterService: ClusterService,
    private val client: Client,
    private val threadPool: ThreadPool
) : AbstractLifecycleComponent(), ClusterStateListener {

    private val log = LogManager.getLogger(javaClass)

    /** Workers keyed by follower ShardId. Bounded by shards-this-node-hosts-for-replicated-indices. */
    private val workers = ConcurrentHashMap<ShardId, ShardWorker>()

    /** Map follower ShardId -> leader index name (UUID resolved lazily in the worker). */
    private data class Desired(val leaderIndexName: String)

    /** Periodic kick so we retry dead workers even when no cluster-state event fires. */
    private var periodic: Scheduler.Cancellable? = null
    private val periodicInterval = TimeValue.timeValueSeconds(2)

    override fun doStart() {
        clusterService.addListener(this)
        periodic = threadPool.scheduleWithFixedDelay(
            {
                try {
                    reconcile()
                } catch (e: Exception) {
                    log.debug("reconciler periodic tick failed: {}", e.message)
                }
            },
            periodicInterval,
            ThreadPool.Names.GENERIC
        )
    }

    override fun doStop() {
        clusterService.removeListener(this)
        periodic?.cancel()
        periodic = null
        workers.values.forEach { it.stop("reconciler stopping") }
        workers.values.forEach { it.close() }
        workers.clear()
    }

    override fun doClose() { /* resources released in doStop */ }

    override fun clusterChanged(event: ClusterChangedEvent) {
        reconcile()
    }

    private fun reconcile() {
        val stateLifecycle = clusterService.lifecycleState()
        if (stateLifecycle != org.opensearch.common.lifecycle.Lifecycle.State.STARTED) return
        val state = clusterService.state()
        val intent = ReplicationIntent.Reader.from(state.metadata)
        if (intent == null || !intent.isSecondary) {
            if (workers.isNotEmpty()) {
                log.info("reconciler: intent null or role!=SECONDARY; stopping all workers")
                workers.forEach { (_, w) -> w.stop("role change or intent cleared") }
                workers.clear()
            }
            return
        }

        val desired: Map<ShardId, Desired> = computeDesiredWorkers(intent)

        // Stop workers for shards no longer in the desired set.
        val toRemove = workers.keys - desired.keys
        for (followerShardId in toRemove) {
            workers.remove(followerShardId)?.let {
                it.stop("no longer assigned to this node / not replicated")
                it.close()
            }
        }

        // Drop dead workers so the restart path below re-creates them. Workers exit early
        // (e.g., shard engine not yet ready, follower index not yet visible) and sit there as
        // no-op entries otherwise.
        val dead = workers.entries.filter { !it.value.isRunning() }.map { it.key }
        for (followerShardId in dead) {
            workers.remove(followerShardId)?.close()
        }

        // Start workers for new entries.
        val clusterName = clusterService.clusterName.value()
        val clusterUUID = state.metadata.clusterUUID()
        for ((followerShardId, d) in desired) {
            if (workers.containsKey(followerShardId)) continue
            val worker = ShardWorker(
                leaderAlias = intent.remoteAlias,
                leaderIndexName = d.leaderIndexName,
                followerShardId = followerShardId,
                client = client,
                clusterName = clusterName,
                clusterUUID = clusterUUID
            )
            workers[followerShardId] = worker
            worker.start()
        }
    }

    /**
     * Desired worker set for this node: every local index that's been marked as a follower
     * (REPLICATED_INDEX_SETTING set by the bootstrap orchestrator at restore time), whose
     * primary shards are assigned here. The leader's index UUID is resolved lazily by the
     * worker via a one-shot remote cluster-state fetch.
     */
    private fun computeDesiredWorkers(@Suppress("UNUSED_PARAMETER") intent: ReplicationIntent): Map<ShardId, Desired> {
        val state = clusterService.state()
        val localNodeId = clusterService.localNode()?.id ?: return emptyMap()
        val routingTable = state.routingTable()
        val result = mutableMapOf<ShardId, Desired>()

        for (indexMetadata in state.metadata.indices().values) {
            if (!ReplicationScope.isReplicatedFollower(indexMetadata)) continue
            val indexName = indexMetadata.index.name
            val indexRoutingTable = routingTable.index(indexName) ?: continue
            val followerIndex: Index = indexMetadata.index

            for ((shardNum, indexShardRoutingTable) in indexRoutingTable.shards()) {
                val primaryRouting = indexShardRoutingTable.primaryShard()
                if (primaryRouting == null || !primaryRouting.assignedToNode()) continue
                if (primaryRouting.currentNodeId() != localNodeId) continue
                val followerShardId = ShardId(followerIndex, shardNum)
                result[followerShardId] = Desired(leaderIndexName = indexName)
            }
        }
        return result
    }

    /** For observability. */
    fun activeWorkerCount(): Int = workers.size
}
