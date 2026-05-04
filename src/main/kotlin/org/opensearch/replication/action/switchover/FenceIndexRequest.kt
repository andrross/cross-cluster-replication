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

enum class FenceUpdateType {
    FENCE, UNFENCE
}

class FenceIndexRequest : AcknowledgedRequest<FenceIndexRequest>, IndicesRequest {

    val indexName: String
    val updateType: FenceUpdateType

    constructor(indexName: String, updateType: FenceUpdateType) : super() {
        this.indexName = indexName
        this.updateType = updateType
    }

    constructor(inp: StreamInput) : super(inp) {
        indexName = inp.readString()
        updateType = inp.readEnum(FenceUpdateType::class.java)
    }

    override fun validate(): ActionRequestValidationException? = null

    override fun indices(): Array<String> = arrayOf(indexName)

    override fun indicesOptions(): IndicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed()

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(indexName)
        out.writeEnum(updateType)
    }
}
