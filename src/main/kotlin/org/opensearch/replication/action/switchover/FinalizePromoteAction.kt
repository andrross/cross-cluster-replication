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
import org.opensearch.action.ActionType
import org.opensearch.action.IndicesRequest
import org.opensearch.action.support.IndicesOptions
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.action.support.clustermanager.AcknowledgedResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput

/**
 * Internal cluster-manager action used by promote's finalize step. Removes
 * INDEX_REPLICATION_BLOCK and REPLICATED_INDEX_SETTING from a freshly-promoted index in
 * one atomic cluster-state update.
 *
 * Kept separate from [PromoteIndexAction] because the index-level promote is a regular
 * HandledTransportAction (it just fans out to shards). Cluster-state mutations must run
 * on the cluster manager, so the index-level action delegates to this one.
 */
class FinalizePromoteAction private constructor() :
    ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        const val NAME = "indices:admin/plugins/replication/switchover/finalize_promote"
        val INSTANCE: FinalizePromoteAction = FinalizePromoteAction()
    }
}

class FinalizePromoteRequest : AcknowledgedRequest<FinalizePromoteRequest>, IndicesRequest {
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
