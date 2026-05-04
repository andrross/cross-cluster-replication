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
import org.opensearch.action.IndicesRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput

/**
 * @param indexName local index to demote
 * @param leaderIndexName what this index will follow after demotion. Recorded in
 *        REPLICATED_INDEX_SETTING so that a subsequent _start knows the peer shape.
 *        Same as the local name in the common case where both clusters use identical
 *        index names.
 */
class DemoteIndexRequest : AcknowledgedRequest<DemoteIndexRequest>, IndicesRequest {

    val indexName: String
    val leaderIndexName: String

    constructor(indexName: String, leaderIndexName: String) : super() {
        this.indexName = indexName
        this.leaderIndexName = leaderIndexName
    }

    constructor(inp: StreamInput) : super(inp) {
        this.indexName = inp.readString()
        this.leaderIndexName = inp.readString()
    }

    override fun validate(): ActionRequestValidationException? = null

    override fun indices(): Array<String> = arrayOf(indexName)
    override fun indicesOptions(): IndicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed()

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(indexName)
        out.writeString(leaderIndexName)
    }
}
