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

package org.opensearch.replication.v2.action.status

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput

class GetReplicationStatusRequest : ClusterManagerNodeReadRequest<GetReplicationStatusRequest> {

    var relationshipId: String? = null

    constructor() : super()

    constructor(relationshipId: String) : super() {
        this.relationshipId = relationshipId
    }

    constructor(inp: StreamInput) : super(inp) {
        relationshipId = inp.readOptionalString()
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeOptionalString(relationshipId)
    }

    override fun validate(): ActionRequestValidationException? {
        if (relationshipId.isNullOrEmpty()) {
            return ValidateActions.addValidationError("relationship_id is required", null)
        }
        return null
    }
}
