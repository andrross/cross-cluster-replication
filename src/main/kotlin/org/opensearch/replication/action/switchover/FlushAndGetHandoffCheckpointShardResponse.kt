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
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder

class FlushAndGetHandoffCheckpointShardResponse(
    val shardId: ShardId,
    val handoffSeqNo: Long,
    val maxSeqNo: Long,
    val primary: Boolean
) : ActionResponse(), ToXContentObject {

    constructor(inp: StreamInput) : this(
        ShardId(inp),
        inp.readLong(),
        inp.readLong(),
        inp.readBoolean()
    )

    override fun writeTo(out: StreamOutput) {
        shardId.writeTo(out)
        out.writeLong(handoffSeqNo)
        out.writeLong(maxSeqNo)
        out.writeBoolean(primary)
    }

    override fun toXContent(builder: XContentBuilder, params: org.opensearch.core.xcontent.ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("index", shardId.indexName)
        builder.field("shard", shardId.id)
        builder.field("handoff_seq_no", handoffSeqNo)
        builder.field("max_seq_no", maxSeqNo)
        builder.field("primary", primary)
        builder.endObject()
        return builder
    }
}
