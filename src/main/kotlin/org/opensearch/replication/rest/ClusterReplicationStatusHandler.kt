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

import org.opensearch.replication.v2.action.status.GetReplicationStatusAction
import org.opensearch.replication.v2.action.status.GetReplicationStatusRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient

/**
 *   GET /_replication/cluster/{relationship_id}/status
 *
 *   Returns live controller state for the replication relationship. 404 if no intent is
 *   configured under the given relationship_id.
 */
class ClusterReplicationStatusHandler : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> = listOf(
        RestHandler.Route(RestRequest.Method.GET, PATH)
    )

    override fun getName(): String = "plugins_replication_cluster_status"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val relationshipId = request.param(RELATIONSHIP_ID_PARAM)
            ?: throw IllegalArgumentException("relationship_id is required in the URL")
        val req = GetReplicationStatusRequest(relationshipId)
        return RestChannelConsumer { channel ->
            client.admin().cluster().execute(GetReplicationStatusAction.INSTANCE, req, RestToXContentListener(channel))
        }
    }

    companion object {
        private const val RELATIONSHIP_ID_PARAM = "relationship_id"
        private const val PATH = "/_replication/cluster/{$RELATIONSHIP_ID_PARAM}/status"
    }
}
