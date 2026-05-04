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

package org.opensearch.replication.rest

import org.opensearch.replication.action.switchover.FenceIndexAction
import org.opensearch.replication.action.switchover.FenceIndexRequest
import org.opensearch.replication.action.switchover.FenceUpdateType
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException

/**
 * Internal REST endpoint used by the phase-1 stubbed coordinator and integration tests
 * to install or remove the switchover fence block on a leader index.
 *
 *   POST   /_plugins/_replication/switchover/{index}/_fence
 *   DELETE /_plugins/_replication/switchover/{index}/_fence
 */
class FenceIndexHandler : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> = listOf(
        RestHandler.Route(RestRequest.Method.POST, "/_plugins/_replication/switchover/{index}/_fence"),
        RestHandler.Route(RestRequest.Method.DELETE, "/_plugins/_replication/switchover/{index}/_fence")
    )

    override fun getName(): String = "plugins_replication_switchover_fence_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indexName = request.param("index")
        val updateType = if (request.method() == RestRequest.Method.DELETE) FenceUpdateType.UNFENCE else FenceUpdateType.FENCE
        val fenceRequest = FenceIndexRequest(indexName, updateType)
        fenceRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", fenceRequest.clusterManagerNodeTimeout())
        )
        return RestChannelConsumer { channel: RestChannel? ->
            client.admin().cluster().execute(FenceIndexAction.INSTANCE, fenceRequest, RestToXContentListener(channel))
        }
    }
}
