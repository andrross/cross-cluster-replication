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

package org.opensearch.replication.v2.action

import org.opensearch.action.ActionRequestValidationException
import org.opensearch.action.ValidateActions
import org.opensearch.action.support.clustermanager.AcknowledgedRequest
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.XContentParser
import org.opensearch.replication.v2.ReplicationIntent

/**
 * Transport request for PutReplicationIntentAction.
 *
 * The peer cluster alias is part of the URL and set by the REST handler, not carried in the
 * request body. The body carries role/epoch/status.
 *
 *   PUT /_replication/cluster/leader
 *   {
 *     "role": "SECONDARY",
 *     "epoch": 1,
 *     "status": "STEADY"
 *   }
 *
 *   DELETE /_replication/cluster/leader
 *
 * The DELETE path sets `clear = true` on the request. `peerClusterAlias` is still required
 * in that case (it names which relationship to clear).
 */
class PutReplicationIntentRequest : AcknowledgedRequest<PutReplicationIntentRequest> {

    var clear: Boolean = false
    var peerClusterAlias: String? = null
    var role: ReplicationIntent.Role? = null
    var epoch: Long = 0L
    var status: ReplicationIntent.Status = ReplicationIntent.Status.STEADY

    constructor() : super()

    constructor(inp: StreamInput) : super(inp) {
        clear = inp.readBoolean()
        peerClusterAlias = inp.readOptionalString()
        val roleName = inp.readOptionalString()
        role = roleName?.let { ReplicationIntent.Role.valueOf(it) }
        epoch = inp.readLong()
        status = ReplicationIntent.Status.valueOf(inp.readString())
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeBoolean(clear)
        out.writeOptionalString(peerClusterAlias)
        out.writeOptionalString(role?.name)
        out.writeLong(epoch)
        out.writeString(status.name)
    }

    override fun validate(): ActionRequestValidationException? {
        var err: ActionRequestValidationException? = null
        if (peerClusterAlias.isNullOrEmpty()) {
            err = ValidateActions.addValidationError("peer_cluster_alias is required", err)
        }
        if (!clear && role == null) {
            err = ValidateActions.addValidationError("role is required for PUT", err)
        }
        return err
    }

    companion object {
        private const val FIELD_ROLE = "role"
        private const val FIELD_EPOCH = "epoch"
        private const val FIELD_STATUS = "status"

        /**
         * Parse the body of a PUT. Peer alias is not parsed from the body — the caller
         * (REST handler) populates it from the URL path.
         */
        fun parseBody(parser: XContentParser, peerClusterAlias: String): PutReplicationIntentRequest {
            val req = PutReplicationIntentRequest()
            req.peerClusterAlias = peerClusterAlias
            if (parser.currentToken() == null) parser.nextToken()
            require(parser.currentToken() == XContentParser.Token.START_OBJECT) {
                "expected START_OBJECT but got ${parser.currentToken()}"
            }
            var currentField: String? = null
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                val token = parser.currentToken()
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentField = parser.currentName()
                } else when (currentField) {
                    FIELD_ROLE -> req.role = ReplicationIntent.Role.valueOf(parser.text().uppercase())
                    FIELD_EPOCH -> req.epoch = parser.longValue()
                    FIELD_STATUS -> req.status = ReplicationIntent.Status.valueOf(parser.text().uppercase())
                }
            }
            return req
        }
    }
}
