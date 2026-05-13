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
 * Lives in cluster state as a Metadata.Custom. The external control plane chooses the
 * relationship ID; both clusters record the same ID and the same pair of aliases. Only the
 * role differs between the two sides.
 *
 * Scope: every user index is replicated. There is no replicated_indices list on this
 * document — which indices are in scope is computed from local cluster state via
 * ReplicationScope.isReplicable(). A future exclude-patterns extension would add a field
 * here and layer on that predicate without changing call sites.
 *
 * Field semantics:
 *   - relationshipId — caller-chosen, stable across the life of the relationship.
 *   - localAlias — this cluster's label (usually its region / deployment name).
 *   - remoteAlias — the peer cluster's label. On the SECONDARY this must match an existing
 *     `cluster.remote.<remoteAlias>` setting; on the PRIMARY it is cosmetic.
 *   - role — PRIMARY or SECONDARY. Flips on switchover or failover.
 *   - phase — STEADY, SWITCHING, or FAILED_OVER.
 *   - epoch — relationship-generation counter. Bumps exactly once per role flip.
 */
data class ReplicationIntent(
    val relationshipId: String,
    val localAlias: String,
    val remoteAlias: String,
    val role: Role,
    val epoch: Long,
    val phase: Phase
) : AbstractNamedDiffable<Metadata.Custom>(), Metadata.Custom {

    enum class Role { PRIMARY, SECONDARY }

    enum class Phase { STEADY, SWITCHING, FAILED_OVER }

    companion object {
        const val NAME = "replication_intent"

        private const val FIELD_RELATIONSHIP_ID = "relationship_id"
        private const val FIELD_LOCAL_ALIAS = "local_alias"
        private const val FIELD_REMOTE_ALIAS = "remote_alias"
        private const val FIELD_ROLE = "role"
        private const val FIELD_EPOCH = "epoch"
        private const val FIELD_PHASE = "phase"

        fun readDiffFrom(inp: StreamInput): NamedDiff<Metadata.Custom> =
            readDiffFrom(Metadata.Custom::class.java, NAME, inp)

        fun fromXContent(parser: XContentParser): ReplicationIntent {
            var relationshipId: String? = null
            var localAlias: String? = null
            var remoteAlias: String? = null
            var role: Role? = null
            var epoch: Long = 0L
            var phase: Phase = Phase.STEADY

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
                    FIELD_RELATIONSHIP_ID -> relationshipId = parser.text()
                    FIELD_LOCAL_ALIAS -> localAlias = parser.text()
                    FIELD_REMOTE_ALIAS -> remoteAlias = parser.text()
                    FIELD_ROLE -> role = Role.valueOf(parser.text().uppercase())
                    FIELD_EPOCH -> epoch = parser.longValue()
                    FIELD_PHASE -> phase = Phase.valueOf(parser.text().uppercase())
                }
            }
            require(relationshipId != null && localAlias != null && remoteAlias != null && role != null) {
                "replication intent requires $FIELD_RELATIONSHIP_ID, $FIELD_LOCAL_ALIAS, $FIELD_REMOTE_ALIAS, $FIELD_ROLE"
            }
            return ReplicationIntent(relationshipId, localAlias, remoteAlias, role, epoch, phase)
        }
    }

    constructor(inp: StreamInput) : this(
        relationshipId = inp.readString(),
        localAlias = inp.readString(),
        remoteAlias = inp.readString(),
        role = Role.valueOf(inp.readString()),
        epoch = inp.readLong(),
        phase = Phase.valueOf(inp.readString())
    )

    override fun writeTo(out: StreamOutput) {
        out.writeString(relationshipId)
        out.writeString(localAlias)
        out.writeString(remoteAlias)
        out.writeString(role.name)
        out.writeLong(epoch)
        out.writeString(phase.name)
    }

    override fun getWriteableName(): String = NAME

    override fun getMinimalSupportedVersion(): Version = Version.V_2_0_0

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        builder.field(FIELD_RELATIONSHIP_ID, relationshipId)
        builder.field(FIELD_LOCAL_ALIAS, localAlias)
        builder.field(FIELD_REMOTE_ALIAS, remoteAlias)
        builder.field(FIELD_ROLE, role.name)
        builder.field(FIELD_EPOCH, epoch)
        builder.field(FIELD_PHASE, phase.name)
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
