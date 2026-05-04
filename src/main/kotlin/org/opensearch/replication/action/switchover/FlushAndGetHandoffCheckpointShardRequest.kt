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

class FlushAndGetHandoffCheckpointShardRequest : SingleShardRequest<FlushAndGetHandoffCheckpointShardRequest> {
    val shardId: ShardId

    constructor(shardId: ShardId) : super(shardId.indexName) {
        this.shardId = shardId
    }

    constructor(inp: StreamInput) : super(inp) {
        this.shardId = ShardId(inp)
    }

    override fun validate(): ActionRequestValidationException? = super.validateNonNullIndex()

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        shardId.writeTo(out)
    }
}
