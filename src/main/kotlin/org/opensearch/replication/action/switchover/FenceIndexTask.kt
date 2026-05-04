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

import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.cluster.AckedClusterStateUpdateTask
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.block.ClusterBlocks
import org.opensearch.core.action.ActionListener

class FenceIndexTask(
    val request: FenceIndexRequest,
    listener: ActionListener<AcknowledgedResponse>
) : AckedClusterStateUpdateTask<AcknowledgedResponse>(request, listener) {

    override fun execute(currentState: ClusterState): ClusterState {
        val newBlocks = ClusterBlocks.builder().blocks(currentState.blocks)
        when (request.updateType) {
            FenceUpdateType.FENCE ->
                if (!currentState.blocks.hasIndexBlock(request.indexName, INDEX_SWITCHOVER_FENCE_BLOCK)) {
                    newBlocks.addIndexBlock(request.indexName, INDEX_SWITCHOVER_FENCE_BLOCK)
                }
            FenceUpdateType.UNFENCE ->
                newBlocks.removeIndexBlock(request.indexName, INDEX_SWITCHOVER_FENCE_BLOCK)
        }
        return ClusterState.builder(currentState).blocks(newBlocks).build()
    }

    override fun newResponse(acknowledged: Boolean) = AcknowledgedResponse(acknowledged)
}
