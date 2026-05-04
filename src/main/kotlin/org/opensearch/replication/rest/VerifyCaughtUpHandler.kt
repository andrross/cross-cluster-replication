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
import org.opensearch.replication.action.switchover.VerifyCaughtUpAction
import org.opensearch.replication.action.switchover.VerifyCaughtUpRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException

/**
 * Verify that the follower index has applied ops through a given set of per-shard target
 * seqnos. Operator-facing — shard-level verification is an internal primitive used by this
 * action and does not get a REST endpoint.
 *
 *   POST /_plugins/_replication/switchover/{index}/_verify_caught_up
 *   body: { "timeout_millis": 60000, "target_seq_nos": { "0": 123, "1": 456, ... } }
 */
class VerifyCaughtUpHandler : BaseRestHandler() {

    companion object {
        private const val DEFAULT_TIMEOUT_MILLIS = 60_000L
    }

    override fun routes(): List<RestHandler.Route> = listOf(
        RestHandler.Route(RestRequest.Method.POST, "/_plugins/_replication/switchover/{index}/_verify_caught_up")
    )

    override fun getName(): String = "plugins_replication_switchover_verify_caught_up_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indexName = request.param("index")
        var timeoutMillis = DEFAULT_TIMEOUT_MILLIS
        val targetSeqNos = mutableMapOf<Int, Long>()

        request.contentOrSourceParamParser().use { parser ->
            require(parser.nextToken() == XContentParser.Token.START_OBJECT) {
                "expected START_OBJECT at root of request body"
            }
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                val fieldName = parser.currentName()
                parser.nextToken()
                when (fieldName) {
                    "timeout_millis" -> timeoutMillis = parser.longValue()
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

        val req = VerifyCaughtUpRequest(indexName, targetSeqNos, timeoutMillis)
        return RestChannelConsumer { channel: RestChannel? ->
            client.execute(VerifyCaughtUpAction.INSTANCE, req, RestToXContentListener(channel))
        }
    }
}
