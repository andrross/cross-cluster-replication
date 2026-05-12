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

import org.opensearch.core.action.ActionResponse
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.ToXContentObject
import org.opensearch.core.xcontent.XContentBuilder

class GetReplicationStatusResponse(
    val relationshipId: String,
    val localAlias: String,
    val remoteAlias: String,
    val loopActive: Boolean,
    val lastAppliedMetadataVersion: Long
) : ActionResponse(), ToXContentObject {

    constructor(inp: StreamInput) : this(
        relationshipId = inp.readString(),
        localAlias = inp.readString(),
        remoteAlias = inp.readString(),
        loopActive = inp.readBoolean(),
        lastAppliedMetadataVersion = inp.readLong()
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(relationshipId)
        out.writeString(localAlias)
        out.writeString(remoteAlias)
        out.writeBoolean(loopActive)
        out.writeLong(lastAppliedMetadataVersion)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.startObject()
        builder.field("relationship_id", relationshipId)
        builder.field("local_alias", localAlias)
        builder.field("remote_alias", remoteAlias)
        builder.field("loop_active", loopActive)
        builder.field("last_applied_metadata_version", lastAppliedMetadataVersion)
        builder.endObject()
        return builder
    }
}
