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

import org.opensearch.replication.action.switchover.FlushAndGetHandoffCheckpointAction
import org.opensearch.replication.action.switchover.FlushAndGetHandoffCheckpointRequest
import org.opensearch.rest.BaseRestHandler
import org.opensearch.rest.RestChannel
import org.opensearch.rest.RestHandler
import org.opensearch.rest.RestRequest
import org.opensearch.rest.action.RestToXContentListener
import org.opensearch.transport.client.node.NodeClient
import java.io.IOException

/**
 * Flush every primary shard of a fenced index and return each shard's durable handoff
 * checkpoint. Operator-facing: per-shard handling is an internal transport primitive used
 * by this action and does not get a REST endpoint.
 *
 *   POST /_plugins/_replication/switchover/{index}/_flush_and_get_handoff_checkpoint
 */
class FlushAndGetHandoffCheckpointHandler : BaseRestHandler() {

    override fun routes(): List<RestHandler.Route> = listOf(
        RestHandler.Route(
            RestRequest.Method.POST,
            "/_plugins/_replication/switchover/{index}/_flush_and_get_handoff_checkpoint"
        )
    )

    override fun getName(): String = "plugins_replication_switchover_flush_and_get_handoff_checkpoint_action"

    @Throws(IOException::class)
    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val indexName = request.param("index")
        val req = FlushAndGetHandoffCheckpointRequest(indexName)
        return RestChannelConsumer { channel: RestChannel? ->
            client.execute(FlushAndGetHandoffCheckpointAction.INSTANCE, req, RestToXContentListener(channel))
        }
    }
}
