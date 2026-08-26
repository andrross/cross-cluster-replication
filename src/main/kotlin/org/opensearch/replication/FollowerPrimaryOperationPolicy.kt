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

package org.opensearch.replication

import org.opensearch.action.index.IndexRequest
import org.opensearch.index.engine.DeletionStrategy
import org.opensearch.index.engine.Engine
import org.opensearch.index.engine.IndexingStrategy
import org.opensearch.index.engine.PrimaryOperationPolicy
import org.opensearch.index.engine.OperationStrategyPlanner
import org.opensearch.index.seqno.SequenceNumbers

/**
 * Indexing policy for a follower shard, whose sequence numbers are assigned by the leader.
 */
object FollowerPrimaryOperationPolicy : PrimaryOperationPolicy {

    /**
     * The leader is the authority for this shard's sequence-number space. Besides using the
     * leader's sequence numbers verbatim, this also makes gap filling on promotion a no-op: the
     * follower must not record no-ops at sequence numbers the leader may not have replicated yet.
     */
    override fun acceptsPreAssignedSeqNos(): Boolean = true

    /**
     * Plans with replica semantics so a compare-and-swap the leader already resolved is not
     * re-evaluated here. This relies on `TransportReplayChangesAction` advancing the shard's
     * max_seq_no_of_updates_or_deletes to the leader's value before each operation is applied;
     * without that, an update to an existing document can be planned as an append-only insert.
     */
    override fun planIndex(
        planner: OperationStrategyPlanner<Engine.Index, IndexingStrategy>,
        index: Engine.Index
    ): IndexingStrategy = planner.planOperationAsNonPrimary(asNonPrimary(index))

    /**
     * Delete counterpart of [planIndex], carrying the same precondition.
     */
    override fun planDelete(
        planner: OperationStrategyPlanner<Engine.Delete, DeletionStrategy>,
        delete: Engine.Delete
    ): DeletionStrategy = planner.planOperationAsNonPrimary(asNonPrimary(delete))

    override fun toString(): String = "ccr-follower"

    /**
     * Re-tags an operation as [Engine.Operation.Origin.REPLICA] for planning purposes.
     *
     * The auto-generated ID timestamp is dropped deliberately. Keeping it would let the planner
     * consider the append-only optimization, whose REPLICA branch asserts `version == 1` and a null
     * version type — neither holds for an operation carrying the leader's external version.
     * `TransportReplayChangesAction` already unsets the timestamp upstream; this makes the planning
     * copy independent of that.
     */
    private fun asNonPrimary(index: Engine.Index) = Engine.Index(
        index.uid(), index.parsedDoc(), index.seqNo(), index.primaryTerm(),
        index.version(), null, Engine.Operation.Origin.REPLICA,
        index.startTime(), IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, index.isRetry,
        SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM
    )

    /**
     * Delete counterpart of [asNonPrimary].
     */
    private fun asNonPrimary(delete: Engine.Delete) = Engine.Delete(
        delete.id(), delete.uid(), delete.seqNo(), delete.primaryTerm(),
        delete.version(), null, Engine.Operation.Origin.REPLICA,
        delete.startTime(), SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
        delete.routing()
    )
}
