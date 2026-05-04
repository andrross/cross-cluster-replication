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

import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder

class VerifyCaughtUpResponse(
    val indexName: String,
    val shardResponses: List<VerifyCaughtUpShardResponse>
) : ActionResponse(), ToXContentObject {

    constructor(inp: StreamInput) : this(
        inp.readString(),
        inp.readList(::VerifyCaughtUpShardResponse)
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(indexName)
        out.writeCollection(shardResponses)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("index", indexName)
        builder.field("shard_count", shardResponses.size)
        builder.startArray("shards")
        for (shardResponse in shardResponses.sortedBy { it.shardId.id }) {
            shardResponse.toXContent(builder, params)
        }
        builder.endArray()
        builder.endObject()
        return builder
    }
}
