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
 * @param indexName follower index name being promoted
 * @param targetSeqNos per-shard target seqnos, typically copied from the corresponding
 *        FlushAndGetHandoffCheckpoint response on the former leader
 */
class PromoteIndexRequest : ActionRequest, IndicesRequest {

    val indexName: String
    val targetSeqNos: Map<Int, Long>

    constructor(indexName: String, targetSeqNos: Map<Int, Long>) : super() {
        this.indexName = indexName
        this.targetSeqNos = targetSeqNos
    }

    constructor(inp: StreamInput) : super(inp) {
        this.indexName = inp.readString()
        val count = inp.readVInt()
        this.targetSeqNos = buildMap(count) {
            repeat(count) { put(inp.readVInt(), inp.readLong()) }
        }
    }

    override fun validate(): ActionRequestValidationException? {
        return if (targetSeqNos.isEmpty()) {
            val ex = ActionRequestValidationException()
            ex.addValidationError("target_seq_nos must not be empty")
            ex
        } else null
    }

    override fun indices(): Array<String> = arrayOf(indexName)
    override fun indicesOptions(): IndicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed()

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(indexName)
        out.writeVInt(targetSeqNos.size)
        for ((k, v) in targetSeqNos) {
            out.writeVInt(k)
            out.writeLong(v)
        }
    }
}
