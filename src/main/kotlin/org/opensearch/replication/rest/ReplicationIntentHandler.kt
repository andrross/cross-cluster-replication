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

import org.opensearch.ResourceNotFoundException
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.core.rest.RestStatus
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.replication.v2.ReplicationIntent
import org.opensearch.replication.v2.action.PutReplicationIntentAction
import org.opensearch.replication.v2.action.PutReplicationIntentRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.BytesRestResponse
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException

/**
 *   PUT    /_replication/cluster/{relationship_id}
 *   GET    /_replication/cluster/{relationship_id}
 *   DELETE /_replication/cluster/{relationship_id}
 *
 * PUT body:
 * ```
 *   {
 *     "role": "PRIMARY" | "SECONDARY",
 *     "local_alias": "us-east-1",
 *     "remote_alias": "us-west-2",
 *     "epoch": 1,
 *     "phase": "STEADY"
 *   }
 * ```
 *
 * GET returns the same fields (plus `relationship_id`). 404 if no intent is configured under
 * this relationship ID. DELETE clears the intent; no body.
 */
class ReplicationIntentHandler(private val clusterService: ClusterService) : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> = listOf(
        RestHandler.Route(RestRequest.Method.PUT, PATH_WITH_ID),
        RestHandler.Route(RestRequest.Method.GET, PATH_WITH_ID),
        RestHandler.Route(RestRequest.Method.DELETE, PATH_WITH_ID)
    )

    override fun getName(): String = "plugins_replication_intent"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val relationshipId = request.param(RELATIONSHIP_ID_PARAM)
            ?: throw IllegalArgumentException("relationship_id is required in the URL")

        return when (request.method()) {
            RestRequest.Method.GET -> handleGet(relationshipId)
            RestRequest.Method.DELETE -> handleDelete(client, relationshipId, request)
            RestRequest.Method.PUT -> handlePut(client, relationshipId, request)
            else -> throw IllegalArgumentException("unsupported method: ${request.method()}")
        }
    }

    private fun handleGet(relationshipId: String): RestChannelConsumer {
        val intent = ReplicationIntent.Reader.from(clusterService.state().metadata)
        return RestChannelConsumer { channel ->
            if (intent == null || intent.relationshipId != relationshipId) {
                channel.sendResponse(
                    BytesRestResponse(
                        channel,
                        RestStatus.NOT_FOUND,
                        ResourceNotFoundException(
                            "no replication intent configured for relationship_id=[$relationshipId]"
                        )
                    )
                )
                return@RestChannelConsumer
            }
            val builder = XContentFactory.jsonBuilder()
            builder.startObject()
            intent.toXContent(builder, ToXContent.EMPTY_PARAMS)
            builder.endObject()
            channel.sendResponse(BytesRestResponse(RestStatus.OK, builder))
        }
    }

    private fun handleDelete(
        client: NodeClient,
        relationshipId: String,
        request: RestRequest
    ): RestChannelConsumer {
        val parsed = PutReplicationIntentRequest().also {
            it.clear = true
            it.relationshipId = relationshipId
        }
        parsed.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", parsed.clusterManagerNodeTimeout())
        )
        return RestChannelConsumer { channel ->
            client.admin().cluster().execute(PutReplicationIntentAction.INSTANCE, parsed, RestToXContentListener(channel))
        }
    }

    private fun handlePut(
        client: NodeClient,
        relationshipId: String,
        request: RestRequest
    ): RestChannelConsumer {
        val parsed: PutReplicationIntentRequest = request.contentOrSourceParamParser().use { parser ->
            PutReplicationIntentRequest.parseBody(parser, relationshipId)
        }
        parsed.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", parsed.clusterManagerNodeTimeout())
        )
        return RestChannelConsumer { channel ->
            client.admin().cluster().execute(PutReplicationIntentAction.INSTANCE, parsed, RestToXContentListener(channel))
        }
    }

    companion object {
        private const val RELATIONSHIP_ID_PARAM = "relationship_id"
        private const val PATH_WITH_ID = "/_replication/cluster/{$RELATIONSHIP_ID_PARAM}"
    }
}
