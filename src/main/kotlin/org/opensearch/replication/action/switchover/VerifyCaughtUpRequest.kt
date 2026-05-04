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

import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.IndicesRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput

/**
 * Verify that the follower has applied operations up to the given per-shard target seqnos.
 *
 * @param indexName follower index name
 * @param targetSeqNos map of shardId -> target seqno (typically produced by calling
 *                     _flush_and_get_handoff_checkpoint on the leader index)
 * @param timeoutMillis how long to wait for each shard to reach its target before giving up
 */
class VerifyCaughtUpRequest : ActionRequest, IndicesRequest {

    val indexName: String
    val targetSeqNos: Map<Int, Long>
    val timeoutMillis: Long

    constructor(indexName: String, targetSeqNos: Map<Int, Long>, timeoutMillis: Long) : super() {
        this.indexName = indexName
        this.targetSeqNos = targetSeqNos
        this.timeoutMillis = timeoutMillis
    }

    constructor(inp: StreamInput) : super(inp) {
        this.indexName = inp.readString()
        val count = inp.readVInt()
        this.targetSeqNos = buildMap(count) {
            repeat(count) { put(inp.readVInt(), inp.readLong()) }
        }
        this.timeoutMillis = inp.readLong()
    }

    override fun validate(): ActionRequestValidationException? {
        return if (targetSeqNos.isEmpty()) {
            val ex = ActionRequestValidationException()
            ex.addValidationError("target_seq_nos must not be empty")
            ex
        } else {
            null
        }
    }

    override fun indices(): Array<String> = arrayOf(indexName)

    override fun indicesOptions(): IndicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed()

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(indexName)
        out.writeVInt(targetSeqNos.size)
        for ((shardId, targetSeqNo) in targetSeqNos) {
            out.writeVInt(shardId)
            out.writeLong(targetSeqNo)
        }
        out.writeLong(timeoutMillis)
    }
}
