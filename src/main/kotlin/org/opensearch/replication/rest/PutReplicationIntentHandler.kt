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

import org.opensearch.replication.v2.action.PutReplicationIntentAction
import org.opensearch.replication.v2.action.PutReplicationIntentRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException

/**
 *   PUT    /_replication/cluster/{peer_cluster_alias}
 *   {
 *     "role": "PRIMARY" | "SECONDARY",
 *     "epoch": 1,
 *     "status": "STEADY"
 *   }
 *
 *   DELETE /_replication/cluster/{peer_cluster_alias}
 */
class PutReplicationIntentHandler : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> = listOf(
        RestHandler.Route(RestRequest.Method.PUT, PATH_WITH_ALIAS),
        RestHandler.Route(RestRequest.Method.DELETE, PATH_WITH_ALIAS)
    )

    override fun getName(): String = "plugins_replication_put_intent"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val peerAlias = request.param(PEER_ALIAS_PARAM)
            ?: throw IllegalArgumentException("peer cluster alias is required in the URL")

        val parsed: PutReplicationIntentRequest = when (request.method()) {
            RestRequest.Method.DELETE -> PutReplicationIntentRequest().also {
                it.clear = true
                it.peerClusterAlias = peerAlias
            }
            RestRequest.Method.PUT -> request.contentOrSourceParamParser().use { parser ->
                PutReplicationIntentRequest.parseBody(parser, peerAlias)
            }
            else -> throw IllegalArgumentException("unsupported method: ${request.method()}")
        }

        parsed.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", parsed.clusterManagerNodeTimeout())
        )
        return RestChannelConsumer { channel ->
            client.admin().cluster().execute(PutReplicationIntentAction.INSTANCE, parsed, RestToXContentListener(channel))
        }
    }

    companion object {
        private const val PEER_ALIAS_PARAM = "peer_cluster_alias"
        private const val PATH_WITH_ALIAS = "/_replication/cluster/{$PEER_ALIAS_PARAM}"
    }
}
