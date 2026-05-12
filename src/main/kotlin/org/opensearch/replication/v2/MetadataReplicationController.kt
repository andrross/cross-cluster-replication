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
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.action.admin.cluster.state.ClusterStateAction
import org.opensearch.action.admin.cluster.state.ClusterStateRequest
import org.opensearch.action.admin.cluster.state.ClusterStateResponse
import org.opensearch.cluster.ClusterChangedEvent
import org.opensearch.cluster.ClusterStateListener
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.lifecycle.AbstractLifecycleComponent
import org.opensearch.common.unit.TimeValue
import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.util.suspendExecute
import org.opensearch.replication.v2.metadata.ApplyResult
import org.opensearch.replication.v2.metadata.CategoryHandler
import org.opensearch.replication.v2.metadata.CategoryHandlerRegistry
import org.opensearch.transport.client.Client

/**
 * Drives metadata replication on the secondary by long-polling the primary's cluster state
 * using `ClusterStateAction` with `waitForMetadataVersion`.
 *
 * On the primary-role cluster this is a no-op — OpenSearch core serves `ClusterStateAction`
 * and there is no per-secondary state to maintain. The component re-evaluates its role on
 * every cluster-state update, starting and stopping the loop as the intent changes.
 *
 * Failure posture is "fail forward" per the design tenet: unknown categories, handler errors,
 * and transient failures all advance `lastAppliedMetadataVersion` (with quarantine counters)
 * rather than stalling the loop.
 */
class MetadataReplicationController(
    private val clusterService: ClusterService,
    private val client: Client,
    private val handlerRegistry: CategoryHandlerRegistry,
    private val replicationMetadataManager: ReplicationMetadataManager,
    private val bootstrap: BootstrapOrchestrator
) : AbstractLifecycleComponent(), ClusterStateListener {

    private val log = LogManager.getLogger(javaClass)

    private val scope = CoroutineScope(
        SupervisorJob() + Dispatchers.Default + CoroutineName("metadata-replication")
    )

    // Guarded by single-threaded access via clusterChanged() on the applier thread.
    private var loopJob: Job? = null
    private var activeLoopKey: LoopKey? = null

    /** Key that determines whether the loop should be restarted. */
    private data class LoopKey(val relationshipId: String, val epoch: Long)

    @Volatile private var lastAppliedMetadataVersion: Long = 0L
    @Volatile private var quarantineCount: Long = 0L
    @Volatile private var unknownCategoryCount: Long = 0L

    /** Default wait the primary holds the response open for (see design doc). */
    private val pollWaitTimeout: TimeValue = TimeValue.timeValueSeconds(30)

    /** Backoff when the peer is unhealthy / not-primary. */
    private val notPrimaryBackoff: TimeValue = TimeValue.timeValueSeconds(5)

    override fun doStart() {
        clusterService.addListener(this)
        // The first cluster-state publication after node start will drive syncRole() via
        // clusterChanged(). Don't call syncRole() here — during doStart() the cluster state
        // isn't initialized yet (ClusterApplierService.state() asserts).
    }

    override fun doStop() {
        clusterService.removeListener(this)
        loopJob?.cancel(CancellationException("MetadataReplicationController stopping"))
        loopJob = null
    }

    override fun doClose() {
        scope.cancel(CancellationException("MetadataReplicationController closing"))
    }

    override fun clusterChanged(event: ClusterChangedEvent) {
        // The intent is a Metadata.Custom — compare old vs new.
        val prev = ReplicationIntent.Reader.from(event.previousState().metadata)
        val curr = ReplicationIntent.Reader.from(event.state().metadata)
        if (prev != curr) {
            log.info("replication intent changed: {} -> {}", prev, curr)
            // NOTE: severing the follower relationship on SECONDARY → not-SECONDARY is driven
            // by TransportPutReplicationIntentAction (the DELETE handler). It runs sever
            // synchronously BEFORE clearing the intent so clients never see a window where
            // the intent is gone but the indices still have REPLICATED_INDEX_SETTING. We
            // intentionally do NOT fire another sever here — doing so would race with the
            // DELETE path and could double-close/open indices.
        }
        syncRole()
    }

    /**
     * Start or stop the long-poll loop to match current intent. Idempotent and cheap; safe to
     * call from any cluster-state-driven callback.
     *
     * The loop is cluster-manager-scoped: only the elected cluster manager runs it. On
     * cluster-manager failover the new leader's next cluster-state event will flip this on and
     * the old leader's will flip it off. Data nodes and non-elected cluster-manager-eligible
     * nodes do nothing here.
     */
    private fun syncRole() {
        val state = clusterService.state()
        if (!state.nodes().isLocalNodeElectedClusterManager) {
            stopLoop("not elected cluster manager")
            return
        }
        val intent = ReplicationIntent.Reader.from(state.metadata)
        when {
            intent == null -> stopLoop("no intent present")
            intent.isSecondary -> ensureLoopForEpoch(intent)
            else -> stopLoop("role=${intent.role}")
        }
    }

    private fun ensureLoopForEpoch(intent: ReplicationIntent) {
        val key = LoopKey(intent.relationshipId, intent.epoch)
        if (loopJob?.isActive == true && activeLoopKey == key) return
        stopLoop("restarting for $key")
        activeLoopKey = key
        // Fresh relationship (new intent, new epoch, or controller just became active) gets a
        // fresh baseline — the peer may have a different metadata version than the one we last
        // saw on a prior relationship, so bootstrap by omitting waitForMetadataVersion on the
        // first iteration.
        lastAppliedMetadataVersion = 0L
        loopJob = scope.launch { runLoop(intent.remoteAlias) }
        log.info("metadata long-poll loop started (relationship_id={}, remote={}, epoch={})",
            intent.relationshipId, intent.remoteAlias, intent.epoch)
    }

    private fun stopLoop(reason: String) {
        loopJob?.let {
            it.cancel(CancellationException("stopping loop: $reason"))
            log.info("metadata long-poll loop stopped: {}", reason)
        }
        loopJob = null
        activeLoopKey = null
    }

    private suspend fun runLoop(remoteAlias: String) {
        val remoteClient = client.getRemoteClusterClient(remoteAlias)

        while (scope.isActive) {
            val intent = ReplicationIntent.Reader.from(clusterService.state().metadata)
            if (intent == null || intent.remoteAlias != remoteAlias || !intent.isSecondary) {
                log.info("loop stopping: intent changed under us ({})", intent)
                return
            }

            // Bootstrap iteration (nothing applied yet): plain fetch of current state. The
            // waitForMetadataVersion gate only makes sense once we have a baseline to wait past.
            val req = ClusterStateRequest()
                .clear()
                .metadata(true)
                .customs(true)
            if (lastAppliedMetadataVersion > 0L) {
                req.waitForMetadataVersion(lastAppliedMetadataVersion + 1)
                req.waitForTimeout(pollWaitTimeout)
            }

            val resp: ClusterStateResponse = try {
                remoteClient.suspendExecute(
                    action = ClusterStateAction.INSTANCE,
                    req = req,
                    injectSecurityContext = true
                )
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("metadata poll to {} failed; retrying after backoff: {}",
                    remoteAlias, e.message)
                delay(notPrimaryBackoff.millis)
                continue
            }

            if (resp.isWaitForTimedOut) {
                // No change in the wait window; re-poll immediately.
                continue
            }

            val primaryState = resp.state
            if (primaryState == null) {
                log.warn("poll returned non-timeout without a state; backoff")
                delay(notPrimaryBackoff.millis)
                continue
            }

            val primaryIntent = ReplicationIntent.Reader.from(primaryState.metadata)
            if (primaryIntent == null || !primaryIntent.isPrimary) {
                log.warn("peer {} is not primary (intent={}); backoff",
                    remoteAlias, primaryIntent?.role)
                delay(notPrimaryBackoff.millis)
                continue
            }

            val primaryVersion = primaryState.metadata.version()
            if (primaryVersion < lastAppliedMetadataVersion) {
                log.warn("peer {} metadata_version regressed ({} < {}); full resync",
                    remoteAlias, primaryVersion, lastAppliedMetadataVersion)
                lastAppliedMetadataVersion = 0L
                continue
            }

            applyIncoming(primaryState)
            lastAppliedMetadataVersion = primaryVersion

            // After applying metadata from the peer, reconcile missing local indices by
            // triggering snapshot restores against whatever the primary currently considers
            // replicable. Each call is idempotent — indices already present or already being
            // restored are skipped.
            try {
                bootstrap.tryBootstrap(intent, primaryState)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("bootstrap sweep failed: {}", e.message)
            }
        }
    }

    private fun applyIncoming(primaryState: org.opensearch.cluster.ClusterState) {
        val localState = clusterService.state()
        for (handler in handlerRegistry.inApplyOrder()) {
            runHandler(handler, localState, primaryState)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T : Any> runHandler(
        handler: CategoryHandler<T>,
        localState: org.opensearch.cluster.ClusterState,
        primaryState: org.opensearch.cluster.ClusterState
    ) {
        val primary: Map<String, T> = try {
            handler.extract(primaryState)
        } catch (e: Exception) {
            unknownCategoryCount += 1
            log.warn("extract(primary) failed for category {}: {}", handler.category(), e.message)
            return
        }
        val local: Map<String, T> = try {
            handler.extract(localState)
        } catch (e: Exception) {
            log.warn("extract(local) failed for category {}: {}", handler.category(), e.message)
            return
        }

        // Upserts: in primary, missing locally or different.
        for ((name, primaryObj) in primary) {
            val localObj = local[name]
            if (localObj != null && handler.equal(localObj, primaryObj)) continue
            when (val result = handler.upsert(name, primaryObj)) {
                is ApplyResult.Success -> { /* no-op */ }
                is ApplyResult.PermanentFailure -> {
                    quarantineCount += 1
                    log.warn("quarantine (permanent) {}/{}: {}",
                        handler.category(), name, result.reason)
                }
                is ApplyResult.TransientFailure -> {
                    quarantineCount += 1
                    log.warn("quarantine (transient→advance) {}/{}: {}",
                        handler.category(), name, result.reason)
                }
            }
        }

        // Deletes: in local, missing from primary.
        for (name in local.keys) {
            if (name in primary) continue
            when (val result = handler.delete(name)) {
                is ApplyResult.Success -> { /* no-op */ }
                is ApplyResult.PermanentFailure -> {
                    quarantineCount += 1
                    log.warn("quarantine-delete (permanent) {}/{}: {}",
                        handler.category(), name, result.reason)
                }
                is ApplyResult.TransientFailure -> {
                    quarantineCount += 1
                    log.warn("quarantine-delete (transient→advance) {}/{}: {}",
                        handler.category(), name, result.reason)
                }
            }
        }
    }

    /** Exposed for observability. */
    fun stats(): Stats = Stats(
        lastAppliedMetadataVersion = lastAppliedMetadataVersion,
        quarantineCount = quarantineCount,
        unknownCategoryCount = unknownCategoryCount,
        active = loopJob?.isActive == true
    )

    data class Stats(
        val lastAppliedMetadataVersion: Long,
        val quarantineCount: Long,
        val unknownCategoryCount: Long,
        val active: Boolean
    )
}
