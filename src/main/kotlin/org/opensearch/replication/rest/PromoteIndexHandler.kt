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
import org.opensearch.replication.action.switchover.PromoteIndexAction
import org.opensearch.replication.action.switchover.PromoteIndexRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException

/**
 *   POST /_plugins/_replication/switchover/{index}/_promote
 *   body: { "target_seq_nos": { "0": 123, "1": 456, ... } }
 */
class PromoteIndexHandler : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> = listOf(
        RestHandler.Route(RestRequest.Method.POST, "/_plugins/_replication/switchover/{index}/_promote")
    )

    override fun getName(): String = "plugins_replication_switchover_promote_index_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indexName = request.param("index")
        val targetSeqNos = mutableMapOf<Int, Long>()

        request.contentOrSourceParamParser().use { parser ->
            require(parser.nextToken() == XContentParser.Token.START_OBJECT) {
                "expected START_OBJECT at root of request body"
            }
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = parser.currentName()
                parser.nextToken()
                when (fieldName) {
                    "target_seq_nos" -> {
                        require(parser.currentToken() == XContentParser.Token.START_OBJECT) {
                            "target_seq_nos must be an object mapping shard id -> seqno"
                        }
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            val shardIdStr = parser.currentName()
                            parser.nextToken()
                            val shardId = shardIdStr.toIntOrNull()
                                ?: throw IllegalArgumentException("shard id '$shardIdStr' is not an integer")
                            targetSeqNos[shardId] = parser.longValue()
                        }
                    }
                    else -> parser.skipChildren()
                }
            }
        }

        val req = PromoteIndexRequest(indexName, targetSeqNos)
        return RestChannelConsumer { channel: RestChannel? ->
            client.execute(PromoteIndexAction.INSTANCE, req, RestToXContentListener(channel))
        }
    }
}
