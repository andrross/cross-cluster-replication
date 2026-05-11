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

package org.opensearch.replication.v2.shard

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.apache.lucene.store.AlreadyClosedException
import org.opensearch.OpenSearchTimeoutException
import org.opensearch.action.admin.cluster.state.ClusterStateAction
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.IndexNotFoundException
import org.opensearch.replication.action.changes.GetChangesAction
import org.opensearch.replication.action.changes.GetChangesRequest
import org.opensearch.replication.action.changes.GetChangesResponse
import org.opensearch.replication.action.replay.ReplayChangesAction
import org.opensearch.replication.action.replay.ReplayChangesRequest
import org.opensearch.replication.seqno.RemoteClusterRetentionLeaseHelper
import org.opensearch.replication.util.indicesService
import org.opensearch.replication.util.suspendExecute
import org.opensearch.transport.NodeNotConnectedException
import org.opensearch.transport.client.Client

/**
 * In-memory per-shard replication worker. Pulls operations from the peer cluster's leader
 * shard, replays them locally. Lifecycle is owned by the per-node reconciler: start when this
 * node hosts the follower primary, stop when it doesn't (shard moved, index removed from
 * intent, role flipped, etc.).
 *
 * No persistent-task entry is created. Worker state lives entirely on this node; failures are
 * observed and corrected by the reconciler on the next cluster-state change.
 *
 * This is a lean first cut:
 *   - single reader per shard (the legacy code uses a semaphore of N readers)
 *   - no TranslogSequencer (operations applied immediately in the order received)
 *   - no dynamic batch-size adjustment
 *
 * These are viable to add later; the scaffolding lives in the existing task.shard package and
 * can be pulled in once the reconciler path is proven end-to-end.
 */
class ShardWorker(
    private val leaderAlias: String,
    /** Leader-side shard: index name known, UUID resolved at startup via remote cluster state. */
    private val leaderIndexName: String,
    private val followerShardId: ShardId,
    private val client: Client,
    private val clusterName: String,
    private val clusterUUID: String
) {
    private val log = LogManager.getLogger(javaClass)

    private val uncaught = CoroutineExceptionHandler { _, e ->
        log.warn("shard worker: uncaught exception (will be restarted by reconciler): {}",
            e.message, e)
    }
    private val scope = CoroutineScope(
        SupervisorJob() + Dispatchers.Default + uncaught +
            CoroutineName("shard-worker[$leaderAlias:$leaderIndexName:${followerShardId.id()}]")
    )
    private var job: Job? = null

    private val batchSize = 512
    private val initialBackoffMillis = 1_000L
    private val maxBackoffMillis = 60_000L
    private val backoffFactor = 2

    fun start() {
        if (job?.isActive == true) return
        log.info("shard worker starting: leader={}:{}, follower={}",
            leaderAlias, leaderIndexName, followerShardId)
        job = scope.launch { runPullLoop() }
    }

    fun stop(reason: String) {
        log.info("shard worker stopping: follower={}, reason={}", followerShardId, reason)
        job?.cancel(CancellationException(reason))
        job = null
    }

    /**
     * Cancel the worker and block until it has actually exited (or timeout elapses). Used by
     * the delete path where the caller is about to remove the follower index and must not race
     * with the pull loop touching shard state.
     */
    fun stopAndJoin(reason: String, timeoutMillis: Long = 5_000) {
        val j = job ?: return
        j.cancel(CancellationException(reason))
        runBlocking {
            withTimeoutOrNull(timeoutMillis) { j.join() }
        }
        job = null
        log.info("shard worker stopped: follower={}, reason={}", followerShardId, reason)
    }

    fun isRunning(): Boolean = job?.isActive == true

    private suspend fun runPullLoop() {
        val remoteClient = client.getRemoteClusterClient(leaderAlias)
        val leaseHelper = RemoteClusterRetentionLeaseHelper(clusterName, clusterUUID, remoteClient)

        val leaderShardId: ShardId = try {
            resolveLeaderShardId(remoteClient)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            log.warn("shard worker: unable to resolve leader {} UUID: {}",
                leaderIndexName, e.message)
            return
        }

        val indexService = indicesService.indexService(followerShardId.index)
        if (indexService == null) {
            log.warn("shard worker: follower index {} not found locally; bailing",
                followerShardId.index)
            return
        }
        val indexShard = indexService.getShardOrNull(followerShardId.id())
        if (indexShard == null) {
            log.warn("shard worker: follower shard {} not found locally; bailing", followerShardId)
            return
        }

        val initialCheckpoint: Long = try {
            indexShard.localCheckpoint
        } catch (e: AlreadyClosedException) {
            log.info("shard worker: shard {} engine not yet ready; reconciler will restart",
                followerShardId)
            return
        }

        try {
            leaseHelper.renewRetentionLease(leaderShardId, indexShard.lastSyncedGlobalCheckpoint + 1, followerShardId)
        } catch (e: AlreadyClosedException) {
            log.info("shard worker: shard {} engine closed during initial lease renewal",
                followerShardId)
            return
        } catch (e: Exception) {
            log.warn("shard worker: initial retention lease renewal failed ({}), continuing", e.message)
        }

        var backoff = initialBackoffMillis
        var fromSeqNo = initialCheckpoint + 1

        while (scope.isActive) {
            val toSeqNo = fromSeqNo + batchSize - 1
            val resp: GetChangesResponse = try {
                remoteClient.suspendExecute(
                    action = GetChangesAction.INSTANCE,
                    req = GetChangesRequest(leaderShardId, fromSeqNo, toSeqNo),
                    injectSecurityContext = true
                )
            } catch (e: CancellationException) {
                throw e
            } catch (e: OpenSearchTimeoutException) {
                // Leader has no new ops — this is the normal long-poll wake-up.
                continue
            } catch (e: NodeNotConnectedException) {
                log.info("shard worker: peer {} unreachable, backoff {}ms", leaderAlias, backoff)
                delay(backoff)
                backoff = (backoff * backoffFactor).coerceAtMost(maxBackoffMillis)
                continue
            } catch (e: Exception) {
                log.warn("shard worker: GetChanges failed (from={}, to={}): {}",
                    fromSeqNo, toSeqNo, e.message)
                delay(backoff)
                backoff = (backoff * backoffFactor).coerceAtMost(maxBackoffMillis)
                continue
            }

            backoff = initialBackoffMillis

            if (resp.changes.isNotEmpty()) {
                try {
                    client.suspendExecute(
                        action = ReplayChangesAction.INSTANCE,
                        req = ReplayChangesRequest(
                            followerShardId,
                            resp.changes,
                            resp.changes.maxOf { it.seqNo() },
                            leaderAlias,
                            leaderShardId.indexName
                        ),
                        injectSecurityContext = true
                    )
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    log.warn("shard worker: ReplayChanges failed: {}", e.message)
                    delay(backoff)
                    backoff = (backoff * backoffFactor).coerceAtMost(maxBackoffMillis)
                    continue
                }
                fromSeqNo = resp.changes.last().seqNo() + 1
            }

            try {
                leaseHelper.renewRetentionLease(
                    leaderShardId,
                    indexShard.lastSyncedGlobalCheckpoint + 1,
                    followerShardId
                )
            } catch (e: AlreadyClosedException) {
                log.info("shard worker: shard {} engine closed; exiting pull loop",
                    followerShardId)
                return
            } catch (e: Exception) {
                log.debug("shard worker: lease renewal hiccup: {}", e.message)
            }
        }
    }

    fun close() {
        scope.cancel(CancellationException("worker closing"))
    }

    private suspend fun resolveLeaderShardId(remoteClient: Client): ShardId {
        val req = ClusterStateRequest()
            .clear()
            .metadata(true)
            .indices(leaderIndexName)
            .indicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
        val resp = remoteClient.suspendExecute(
            action = ClusterStateAction.INSTANCE,
            req = req,
            injectSecurityContext = true
        )
        val leaderMeta = resp.state?.metadata?.index(leaderIndexName)
            ?: throw IndexNotFoundException("$leaderAlias:$leaderIndexName")
        return ShardId(Index(leaderIndexName, leaderMeta.indexUUID), followerShardId.id())
    }
}
