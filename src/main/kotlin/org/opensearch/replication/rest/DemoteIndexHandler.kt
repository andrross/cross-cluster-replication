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

import org.opensearch.core.xcontent.XContentParser
import org.opensearch.replication.action.switchover.DemoteIndexAction
import org.opensearch.replication.action.switchover.DemoteIndexRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException

/**
 *   POST /_plugins/_replication/switchover/{index}/_demote
 *   body: { "leader_index": "peer_index_name" }
 *
 * The body's leader_index names the peer leader index this index will follow after
 * demotion. Usually the same as the local index name; made explicit to support mirrored
 * replication where names might differ.
 */
class DemoteIndexHandler : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> = listOf(
        RestHandler.Route(RestRequest.Method.POST, "/_plugins/_replication/switchover/{index}/_demote")
    )

    override fun getName(): String = "plugins_replication_switchover_demote_index_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indexName = request.param("index")
        var leaderIndex = indexName // default: assume matching names on both sides

        if (request.hasContent()) {
            request.contentOrSourceParamParser().use { parser ->
                require(parser.nextToken() == XContentParser.Token.START_OBJECT) {
                    "expected START_OBJECT at root of request body"
                }
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    val fieldName = parser.currentName()
                    parser.nextToken()
                    when (fieldName) {
                        "leader_index" -> leaderIndex = parser.text()
                        else -> parser.skipChildren()
                    }
                }
            }
        }

        val req = DemoteIndexRequest(indexName, leaderIndex)
        return RestChannelConsumer { channel: RestChannel? ->
            client.admin().cluster().execute(DemoteIndexAction.INSTANCE, req, RestToXContentListener(channel))
        }
    }
}
