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

package org.opensearch.replication.v2

import org.opensearch.Version
import org.opensearch.cluster.AbstractNamedDiffable
import org.opensearch.cluster.NamedDiff
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.core.common.io.stream.StreamInput
import org.opensearch.core.common.io.stream.StreamOutput
import org.opensearch.core.xcontent.ToXContent
import org.opensearch.core.xcontent.XContentBuilder
import org.opensearch.core.xcontent.XContentParser
import java.util.EnumSet

/**
 * Cluster-scoped declarative intent for full-cluster replication.
 *
 * Lives in cluster state as a Metadata.Custom. The external control plane writes the stable
 * fields (peer, role). In-cluster workflows write the transitional fields (status, epoch)
 * during switchover/failover. Everything else in the cluster is a reader.
 *
 * Scope: every user index is replicated. There is no replicated_indices list on this
 * document — which indices are in scope is computed from local cluster state via
 * ReplicationScope.isReplicable(). A future exclude-patterns extension would add a field
 * here and layer on that predicate without changing call sites.
 */
data class ReplicationIntent(
    val peerClusterAlias: String,
    val role: Role,
    val epoch: Long,
    val status: Status
) : AbstractNamedDiffable<Metadata.Custom>(), Metadata.Custom {

    enum class Role { PRIMARY, SECONDARY }

    enum class Status { STEADY, SWITCHING, FAILING_OVER, FAILED_BACK, ABORTED }

    companion object {
        const val NAME = "replication_intent"

        private const val FIELD_PEER = "peer_cluster"
        private const val FIELD_ROLE = "role"
        private const val FIELD_EPOCH = "epoch"
        private const val FIELD_STATUS = "status"

        fun readDiffFrom(inp: StreamInput): NamedDiff<Metadata.Custom> =
            readDiffFrom(Metadata.Custom::class.java, NAME, inp)

        fun fromXContent(parser: XContentParser): ReplicationIntent {
            var peer: String? = null
            var role: Role? = null
            var epoch: Long = 0L
            var status: Status = Status.STEADY

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
                    FIELD_PEER -> peer = parser.text()
                    FIELD_ROLE -> role = Role.valueOf(parser.text().uppercase())
                    FIELD_EPOCH -> epoch = parser.longValue()
                    FIELD_STATUS -> status = Status.valueOf(parser.text().uppercase())
                }
            }
            require(peer != null && role != null) {
                "replication intent requires $FIELD_PEER and $FIELD_ROLE"
            }
            return ReplicationIntent(peer, role, epoch, status)
        }
    }

    constructor(inp: StreamInput) : this(
        peerClusterAlias = inp.readString(),
        role = Role.valueOf(inp.readString()),
        epoch = inp.readLong(),
        status = Status.valueOf(inp.readString())
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(peerClusterAlias)
        out.writeString(role.name)
        out.writeLong(epoch)
        out.writeString(status.name)
    }

    override fun getWriteableName(): String = NAME

    override fun getMinimalSupportedVersion(): Version = Version.V_2_0_0

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.field(FIELD_PEER, peerClusterAlias)
        builder.field(FIELD_ROLE, role.name)
        builder.field(FIELD_EPOCH, epoch)
        builder.field(FIELD_STATUS, status.name)
        return builder
    }

    override fun context(): EnumSet<Metadata.XContentContext> = Metadata.ALL_CONTEXTS

    /** True if this cluster should be operating in the "serve polls" (primary) role. */
    val isPrimary: Boolean get() = role == Role.PRIMARY

    /** True if this cluster should be running the long-poll loop against the peer. */
    val isSecondary: Boolean get() = role == Role.SECONDARY

    /** Convenience read for any component that wants the intent from current cluster state. */
    object Reader {
        fun from(metadata: Metadata): ReplicationIntent? = metadata.custom(NAME)
    }
}
