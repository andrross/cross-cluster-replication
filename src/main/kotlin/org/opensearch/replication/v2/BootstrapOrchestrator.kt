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

package org.opensearch.replication.v2

import kotlinx.coroutines.CancellationException
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchTimeoutException
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.RestoreInProgress
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.repository.REMOTE_SNAPSHOT_NAME
import org.opensearch.replication.repository.RemoteClusterRepository
import org.opensearch.replication.util.suspending
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.util.waitForNextChange
import org.opensearch.transport.client.Client
import java.util.concurrent.ConcurrentHashMap

/**
 * Snapshot-restore bootstrap.
 *
 * On the cluster manager only, for each index in `intent.replicatedIndices` that doesn't exist
 * locally yet, issue a `RestoreSnapshotAction` against the per-peer internal repository
 * (`replication-remote-repo-<peer_alias>`, which is registered automatically as soon as remote
 * cluster seeds are configured — see `RemoteClusterRepositoriesService`).
 *
 * Once the restore lands, the follower index exists locally; the per-node reconciler picks up
 * the new shards on its next cluster-state event and starts in-memory workers for them.
 *
 * The orchestrator is fail-forward: a bootstrap that fails is logged + counted; the next poll
 * cycle will retry. No stuck state is persisted.
 *
 * The `inFlight` set prevents duplicate RestoreSnapshotAction invocations for the same index.
 * On cluster-manager failover the set is lost but `RestoreInProgress` in cluster state acts as
 * the authoritative de-dup: tryBootstrap skips indices already being restored.
 */
class BootstrapOrchestrator(
    private val clusterService: ClusterService,
    private val client: Client,
    private val replicationMetadataManager: ReplicationMetadataManager
) {
    private val log = LogManager.getLogger(javaClass)

    private val inFlight = ConcurrentHashMap.newKeySet<String>()
    private val deleteInFlight = ConcurrentHashMap.newKeySet<String>()

    /**
     * Ensure every index the primary considers replicable is present locally. The primary's
     * scope comes from its own cluster state (the one we just long-polled) filtered via
     * ReplicationScope. Indices that don't exist locally and aren't already being restored get a fresh
     * RestoreSnapshotAction.
     *
     * Called from the long-poll apply path on the cluster manager. Idempotent; safe to call on
     * every iteration.
     */
    suspend fun tryBootstrap(intent: ReplicationIntent, primaryState: ClusterState) {
        val localState = clusterService.state()
        val repoName = RemoteClusterRepository.repoForCluster(intent.peerClusterAlias)

        val primaryIndices: Set<String> = primaryState.metadata.indices()
            .values
            .filter { ReplicationScope.isReplicable(it) }
            .map { it.index.name }
            .toSet()

        // Add missing indices, but only if the leader has finished allocating the index's
        // primary shards. A freshly-created index on the leader may be in YELLOW state with
        // empty inSyncAllocationIds for ~100ms while its primary is being assigned; restoring
        // in that window produces follower IndexMetadata with empty inSyncAllocationIds, which
        // crashes the follower's primary-shard allocator assertion. Deferring to the next
        // long-poll iteration is cheap — a fresh metadata version bumps once the leader's
        // allocation completes.
        for (index in primaryIndices) {
            if (localState.metadata.hasIndex(index)) continue
            if (inProgressRestore(localState, repoName, index) != null) continue
            if (!isLeaderIndexAllocated(primaryState, index)) {
                log.debug("bootstrap: leader {} not yet allocated; deferring", index)
                continue
            }
            if (!inFlight.add(index)) continue
            try {
                bootstrapOne(intent.peerClusterAlias, repoName, index)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("bootstrap for {} failed: {}", index, e.message)
            } finally {
                inFlight.remove(index)
            }
        }

        // Remove follower indices the primary no longer considers replicable. This covers
        // three cases: an index was deleted on the primary; an index was reconfigured to be
        // non-replicable (e.g., hidden); or residual state from a prior relationship that the
        // primary doesn't own.
        val localFollowerIndices = localState.metadata.indices()
            .values
            .filter { ReplicationScope.isReplicatedFollower(it) }
            .map { it.index.name }

        for (index in localFollowerIndices) {
            if (index in primaryIndices) continue
            if (!deleteInFlight.add(index)) continue
            try {
                deleteFollowerIndex(index)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("delete of {} failed: {}", index, e.message)
            } finally {
                deleteInFlight.remove(index)
            }
        }
    }

    /**
     * Remove every local follower index. Used when the intent is cleared or the role flips
     * away from SECONDARY — the relationship is over and the follower data is no longer under
     * replication management.
     */
    suspend fun removeAllFollowerIndices() {
        val localState = clusterService.state()
        val indices = localState.metadata.indices()
            .values
            .filter { ReplicationScope.isReplicatedFollower(it) }
            .map { it.index.name }

        for (index in indices) {
            if (!deleteInFlight.add(index)) continue
            try {
                deleteFollowerIndex(index)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("delete (bulk) of {} failed: {}", index, e.message)
            } finally {
                deleteInFlight.remove(index)
            }
        }
    }

    private suspend fun deleteFollowerIndex(indexName: String) {
        log.info("delete: removing follower index {}", indexName)
        val req = DeleteIndexRequest(indexName)
        try {
            client.suspending(client.admin().indices()::delete, defaultContext = true)(req)
        } catch (e: org.opensearch.index.IndexNotFoundException) {
            // Already gone; idempotent.
        }
        // Best-effort cleanup of the legacy metadata store doc we wrote during bootstrap.
        try {
            replicationMetadataManager.deleteIndexReplicationMetadata(indexName)
        } catch (e: Exception) {
            log.debug("delete: replication-metadata-store cleanup for {} failed: {}",
                indexName, e.message)
        }
    }

    private suspend fun bootstrapOne(peerAlias: String, repoName: String, indexName: String) {
        log.info("bootstrap: restoring {} from peer {} via repo {}", indexName, peerAlias, repoName)

        // The RemoteClusterRepository's restoreShard path reads per-index ReplicationMetadata
        // from the .replication-metadata-store system index (connection name, leader index,
        // settings). Legacy ReplicateIndexAction writes it; we do it here.
        try {
            replicationMetadataManager.addIndexReplicationMetadata(
                followerIndex = indexName,
                connectionName = peerAlias,
                leaderIndex = indexName,
                overallState = ReplicationOverallState.RUNNING,
                user = null,
                follower_cluster_role = null,
                leader_cluster_role = null,
                settings = Settings.EMPTY
            )
        } catch (e: Exception) {
            // Idempotent: if already present from a prior bootstrap pass, keep going.
            log.debug("bootstrap: addIndexReplicationMetadata for {} (may already exist): {}",
                indexName, e.message)
        }

        // REPLICATED_INDEX_SETTING marks the restored index as a follower so
        // ReplicationPlugin.getEngineFactory() installs ReplicationEngine (read-only, follower
        // translog semantics) instead of the default InternalEngine. Without it, ReplayChanges
        // would misbehave against a writable engine.
        val followerIndexSettings = Settings.builder()
            .put(ReplicationPlugin.REPLICATED_INDEX_SETTING.key, "$peerAlias:$indexName")
            .build()

        val restoreRequest = RestoreSnapshotRequest(repoName, REMOTE_SNAPSHOT_NAME)
            .indices(indexName)
            .indexSettings(followerIndexSettings)
            .waitForCompletion(false)
            .aliasWriteIndexPolicy(RestoreSnapshotRequest.AliasWriteIndexPolicy.STRIP_WRITE_INDEX)

        val resp: RestoreSnapshotResponse = client.suspendExecute(
            action = RestoreSnapshotAction.INSTANCE,
            req = restoreRequest,
            defaultContext = true
        )

        // If waitForCompletion=false, restoreInfo is null and the entry shows up in cluster
        // state as RestoreInProgress. Wait for it to reach a terminal state.
        if (resp.restoreInfo != null) {
            val failed = resp.restoreInfo.failedShards()
            if (failed != 0) {
                log.warn("bootstrap: restore of {} had {} failed shards", indexName, failed)
            } else {
                log.info("bootstrap: {} restored (inline)", indexName)
            }
            return
        }

        awaitRestoreComplete(repoName, indexName)
    }

    private suspend fun awaitRestoreComplete(repoName: String, indexName: String) {
        val observer = org.opensearch.cluster.ClusterStateObserver(
            clusterService,
            TimeValue.timeValueMinutes(10),
            log,
            client.threadPool().threadContext
        )
        var entry = inProgressRestore(clusterService.state(), repoName, indexName)
        while (entry != null &&
            entry.state() != RestoreInProgress.State.SUCCESS &&
            entry.state() != RestoreInProgress.State.FAILURE
        ) {
            try {
                observer.waitForNextChange("bootstrap $indexName")
            } catch (e: OpenSearchTimeoutException) {
                log.debug("bootstrap: still waiting on restore of {}", indexName)
            }
            entry = inProgressRestore(clusterService.state(), repoName, indexName)
        }
        when (entry?.state()) {
            RestoreInProgress.State.SUCCESS -> log.info("bootstrap: {} restored", indexName)
            RestoreInProgress.State.FAILURE -> log.warn("bootstrap: {} restore FAILED", indexName)
            null -> {
                // Entry disappeared without transitioning — in legacy code this is interpreted
                // as a completed restore with the entry already cleaned up. Accept that.
                log.info("bootstrap: {} restore entry cleared", indexName)
            }
            else -> { /* unreachable */ }
        }
    }

    /**
     * True iff every primary shard of `indexName` on the leader has a non-empty
     * inSyncAllocationIds entry. Restoring before this is populated produces a follower
     * IndexMetadata whose inSyncAllocationIds is empty, which trips the primary-shard
     * allocator's assertion on the follower.
     */
    private fun isLeaderIndexAllocated(primaryState: ClusterState, indexName: String): Boolean {
        val indexMetadata = primaryState.metadata.index(indexName) ?: return false
        val numShards = indexMetadata.numberOfShards
        for (shardId in 0 until numShards) {
            if (indexMetadata.inSyncAllocationIds(shardId).isEmpty()) return false
        }
        return true
    }

    private fun inProgressRestore(
        cs: ClusterState,
        repoName: String,
        indexName: String
    ): RestoreInProgress.Entry? {
        val ri: RestoreInProgress = cs.custom(RestoreInProgress.TYPE) ?: return null
        return ri.singleOrNull { entry ->
            entry.snapshot().repository == repoName &&
                entry.indices().any { it == indexName }
        }
    }
}
