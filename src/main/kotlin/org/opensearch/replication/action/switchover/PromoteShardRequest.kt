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

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.support.single.shard.SingleShardRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.index.shard.ShardId

class PromoteShardRequest : SingleShardRequest<PromoteShardRequest> {
    val shardId: ShardId
    val targetSeqNo: Long

    constructor(shardId: ShardId, targetSeqNo: Long) : super(shardId.indexName) {
        this.shardId = shardId
        this.targetSeqNo = targetSeqNo
    }

    constructor(inp: StreamInput) : super(inp) {
        this.shardId = ShardId(inp)
        this.targetSeqNo = inp.readLong()
    }

    override fun validate(): ActionRequestValidationException? = super.validateNonNullIndex()

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        shardId.writeTo(out)
        out.writeLong(targetSeqNo)
    }
}
