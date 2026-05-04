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

package org.opensearch.replication.action.index

import org.opensearch.replication.metadata.ReplicationMetadataManager
import org.opensearch.replication.metadata.ReplicationOverallState
import org.opensearch.replication.task.ReplicationState
import org.opensearch.replication.task.index.IndexReplicationExecutor
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.replication.task.index.IndexReplicationState
import org.opensearch.replication.util.coroutineContext
import org.opensearch.replication.util.startTask
import org.opensearch.replication.util.suspending
import org.opensearch.replication.util.waitForTaskCondition
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.opensearch.OpenSearchStatusException
import org.opensearch.core.action.ActionListener
import org.opensearch.action.support.ActionFilters
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction
import org.opensearch.transport.client.node.NodeClient
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlockException
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.IndexNameExpressionResolver
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.inject.Inject
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.common.settings.IndexScopedSettings
import org.opensearch.index.IndexNotFoundException
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.replication.ReplicationPlugin
import org.opensearch.replication.metadata.INDEX_REPLICATION_BLOCK
import org.opensearch.replication.util.stackTraceToString
import org.opensearch.replication.util.suspendExecute
import org.opensearch.repositories.RepositoriesService
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.seqno.RetentionLeaseActions
import org.opensearch.index.seqno.RetentionLeaseAlreadyExistsException
import org.opensearch.core.rest.RestStatus
import org.opensearch.threadpool.ThreadPool
import org.opensearch.transport.TransportService
import java.io.IOException

class TransportReplicateIndexClusterManagerNodeAction @Inject constructor(transportService: TransportService,
                                                                          clusterService: ClusterService,
                                                                          threadPool: ThreadPool,
                                                                          actionFilters: ActionFilters,
                                                                          indexNameExpressionResolver: IndexNameExpressionResolver,
                                                                          val indexScopedSettings: IndexScopedSettings,
                                                                          private val persistentTasksService: PersistentTasksService,
                                                                          private val nodeClient : NodeClient,
                                                                          private val repositoryService: RepositoriesService,
                                                                          private val replicationMetadataManager: ReplicationMetadataManager) :
    TransportClusterManagerNodeAction<ReplicateIndexClusterManagerNodeRequest, AcknowledgedResponse>(ReplicateIndexClusterManagerNodeAction.NAME,
                transportService, clusterService, threadPool, actionFilters, ::ReplicateIndexClusterManagerNodeRequest, indexNameExpressionResolver),
        CoroutineScope by GlobalScope {

    companion object {
        private val log = LogManager.getLogger(TransportReplicateIndexClusterManagerNodeAction::class.java)
    }

    override fun executor(): String {
        return ThreadPool.Names.SAME
    }

    @Throws(IOException::class)
    override fun read(input: StreamInput): ReplicateIndexResponse {
        return ReplicateIndexResponse(input)
    }

    @Throws(Exception::class)
    override fun clusterManagerOperation(request: ReplicateIndexClusterManagerNodeRequest, state: ClusterState,
                                 listener: ActionListener<AcknowledgedResponse>) {
        val replicateIndexReq = request.replicateIndexReq
        val user = request.user
        log.trace("Triggering relevant tasks to start replication for " +
                "${replicateIndexReq.leaderAlias}:${replicateIndexReq.leaderIndex} -> ${replicateIndexReq.followerIndex}")

        // For now this returns a response after creating the follower index and starting the replication tasks
        // for each shard. If that takes too long we can start the task asynchronously and return the response first.
        launch(Dispatchers.Unconfined + threadPool.coroutineContext()) {
            try {
                if(clusterService.clusterSettings.get(ReplicationPlugin.REPLICATION_FOLLOWER_BLOCK_START)) {
                    log.debug("Replication cannot be started as " +
                            "start block(${ReplicationPlugin.REPLICATION_FOLLOWER_BLOCK_START}) is set")
                    throw OpenSearchStatusException("[FORBIDDEN] Replication START block is set", RestStatus.FORBIDDEN)
                }

                log.debug("Making request to get metadata of ${replicateIndexReq.leaderIndex} index on remote cluster")
                val remoteMetadata = getRemoteIndexMetadata(replicateIndexReq.leaderAlias, replicateIndexReq.leaderIndex)
                log.debug("Response returned of the request made to get metadata of ${replicateIndexReq.leaderIndex} index on remote cluster")

                // Three possible states of the follower index, detected from cluster state:
                //   1. Doesn't exist          → normal bootstrap path (snapshot restore).
                //   2. Exists in follower shape (REPLICATED_INDEX_SETTING + CCR block)
                //                             → resume path; skip bootstrap and establish
                //                               retention leases at seqno 0.
                //   3. Exists but not in follower shape → reject. Either this is a plain
                //                               index the caller should delete first, or a
                //                               half-broken state from a failed _stop.
                //
                // The downstream IndexReplicationTask's isResumed() check (which is true
                // whenever the follower index already exists in routing) handles the
                // code-path split on its side; here we just gate and prepare.
                val followerIndexExists = state.routingTable.hasIndex(replicateIndexReq.followerIndex)
                val isResumableFollower = followerIndexExists &&
                    state.metadata.index(replicateIndexReq.followerIndex)
                        ?.settings?.get(ReplicationPlugin.REPLICATED_INDEX_SETTING.key) != null &&
                    state.blocks.hasIndexBlock(replicateIndexReq.followerIndex, INDEX_REPLICATION_BLOCK)

                if (followerIndexExists && !isResumableFollower) {
                    throw IllegalArgumentException("Cant use same index again for replication. " +
                    "Delete the index:${replicateIndexReq.followerIndex}")
                }

                indexScopedSettings.validate(replicateIndexReq.settings,
                        false,
                        false)

                if (isResumableFollower) {
                    // Bootstrap would normally establish the retention lease on each
                    // leader shard via RemoteClusterRestoreLeaderService. Resuming against
                    // an already-demoted follower skips bootstrap, so no lease exists and
                    // the first renewRetentionLease from the shard task would fail. Add
                    // the leases here at seqno 0; the shard task renews them forward on
                    // its first cycle.
                    log.info("Follower index ${replicateIndexReq.followerIndex} already exists " +
                        "in follower shape; skipping bootstrap and establishing retention leases")
                    establishInitialRetentionLeases(
                        replicateIndexReq.leaderAlias,
                        remoteMetadata.index,
                        replicateIndexReq.followerIndex
                    )
                }

                val params = IndexReplicationParams(replicateIndexReq.leaderAlias, remoteMetadata.index, replicateIndexReq.followerIndex)

                replicationMetadataManager.addIndexReplicationMetadata(replicateIndexReq.followerIndex,
                        replicateIndexReq.leaderAlias, replicateIndexReq.leaderIndex,
                        ReplicationOverallState.RUNNING, user, replicateIndexReq.useRoles?.getOrDefault(ReplicateIndexRequest.FOLLOWER_CLUSTER_ROLE, null),
                        replicateIndexReq.useRoles?.getOrDefault(ReplicateIndexRequest.LEADER_CLUSTER_ROLE, null), replicateIndexReq.settings)

                log.debug("Starting index replication task in persistent task service with name: replication:index:${replicateIndexReq.followerIndex}")
                val task = persistentTasksService.startTask("replication:index:${replicateIndexReq.followerIndex}",
                        IndexReplicationExecutor.TASK_NAME, params)

                if (!task.isAssigned) {
                    log.error("Failed to assign task")
                    listener.onResponse(ReplicateIndexResponse(false))
                }

                log.info("Persistent task created for replication: follower=${replicateIndexReq.followerIndex}, leader=${replicateIndexReq.leaderAlias}:${replicateIndexReq.leaderIndex}, taskId=${task.id}")
                log.debug("Waiting for persistent task to move to following state")
                // Now wait for the replication to start and the follower index to get created before returning
                persistentTasksService.waitForTaskCondition(task.id, replicateIndexReq.timeout()) { t ->
                    val replicationState = (t.state as IndexReplicationState?)?.state
                    replicationState == ReplicationState.FOLLOWING ||
                            (!replicateIndexReq.waitForRestore && replicationState == ReplicationState.RESTORING)
                }
                log.debug("Persistent task is moved to following replication state")
                listener.onResponse(AcknowledgedResponse(true))
            } catch (e: Exception) {
                log.error("Failed to trigger replication for ${replicateIndexReq.followerIndex} - ${e.stackTraceToString()}")
                listener.onFailure(e)
            }
        }
    }

    /**
     * Establish initial retention leases on each leader shard so the shard-level pull
     * task's first renew call succeeds. Uses seqno 0 as the starting position; the task
     * will renew forward to the actual local checkpoint on its first cycle.
     *
     * Called only on the skip-bootstrap path. The normal bootstrap path does this inside
     * the snapshot-restore flow.
     */
    private suspend fun establishInitialRetentionLeases(
        leaderAlias: String,
        leaderIndex: org.opensearch.core.index.Index,
        followerIndex: String
    ) {
        val remoteClient = nodeClient.getRemoteClusterClient(leaderAlias)
        val followerRouting = clusterService.state().routingTable.index(followerIndex)
            ?: throw IllegalStateException("Follower index $followerIndex has no routing")
        // Retention lease ID format matches what ShardReplicationTask / RemoteClusterRetentionLeaseHelper
        // use when it later renews: "replication:<follower-cluster>:<follower-uuid>:<follower-shard>".
        val followerClusterNameWithUUID =
            "${clusterService.clusterName.value()}:${clusterService.state().metadata.clusterUUID()}"
        val leaseSource = "replication:$followerClusterNameWithUUID"
        for (shardIdInt in followerRouting.shards().keys) {
            val followerShardId = followerRouting.shard(shardIdInt).shardId
            val leaderShardId = ShardId(leaderIndex, shardIdInt)
            val retentionLeaseId = "$leaseSource:$followerShardId"
            val addRequest = RetentionLeaseActions.AddRequest(
                leaderShardId, retentionLeaseId, 0L, leaseSource
            )
            try {
                remoteClient.suspendExecute(RetentionLeaseActions.Add.INSTANCE, addRequest)
                log.info("Established initial retention lease $retentionLeaseId on leader $leaderAlias:$leaderShardId")
            } catch (e: Exception) {
                // The lease may already exist from the prior replication direction —
                // flips don't currently clean up leases on the former leader. If so,
                // leave it alone: it's retaining at some seqno from prior replication
                // (>= 0), which still serves as a valid floor. The shard task's first
                // renew cycle will advance it forward. Renewing to 0 here would be
                // rejected, since leases can only move forward.
                val isAlreadyExists = e is RetentionLeaseAlreadyExistsException ||
                    e.cause is RetentionLeaseAlreadyExistsException
                if (!isAlreadyExists) {
                    log.warn("Failed to establish initial retention lease for $followerShardId " +
                        "against leader $leaderAlias:$leaderShardId: ${e.message}")
                    throw e
                }
                log.info("Retention lease $retentionLeaseId already exists on $leaderAlias:$leaderShardId — leaving prior lease in place (will be renewed forward by shard task)")
            }
        }
    }

    private suspend fun getRemoteIndexMetadata(leaderAlias: String, leaderIndex: String): IndexMetadata {
        val remoteClusterClient = nodeClient.getRemoteClusterClient(leaderAlias)
        val clusterStateRequest = remoteClusterClient.admin().cluster().prepareState()
                .clear()
                .setIndices(leaderIndex)
                .setMetadata(true)
                .setIndicesOptions(IndicesOptions.strictSingleIndexNoExpandForbidClosed())
                .request()
        val remoteState = remoteClusterClient.suspending(remoteClusterClient.admin().cluster()::state,
                injectSecurityContext = true, defaultContext = true)(clusterStateRequest).state
        return remoteState.metadata.index(leaderIndex) ?: throw IndexNotFoundException("${leaderAlias}:${leaderIndex}")
    }

    override fun checkBlock(request: ReplicateIndexClusterManagerNodeRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks.globalBlockedException(ClusterBlockLevel.METADATA_WRITE)
    }
}
