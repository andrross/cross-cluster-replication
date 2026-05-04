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
import org.opensearch.core.index.shard.ShardId
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder

class PromoteShardResponse(
    val shardId: ShardId,
    val localCheckpoint: Long
) : ActionResponse(), ToXContentObject {

    constructor(inp: StreamInput) : this(ShardId(inp), inp.readLong())

    override fun writeTo(out: StreamOutput) {
        shardId.writeTo(out)
        out.writeLong(localCheckpoint)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("index", shardId.indexName)
        builder.field("shard", shardId.id)
        builder.field("local_checkpoint", localCheckpoint)
        builder.endObject()
        return builder
    }
}
