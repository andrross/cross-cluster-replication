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

class FlushAndGetHandoffCheckpointRequest : ActionRequest, IndicesRequest {

    val indexName: String

    constructor(indexName: String) : super() {
        this.indexName = indexName
    }

    constructor(inp: StreamInput) : super(inp) {
        this.indexName = inp.readString()
    }

    override fun validate(): ActionRequestValidationException? = null

    override fun indices(): Array<String> = arrayOf(indexName)

    override fun indicesOptions(): IndicesOptions = IndicesOptions.strictSingleIndexNoExpandForbidClosed()

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(indexName)
    }
}
