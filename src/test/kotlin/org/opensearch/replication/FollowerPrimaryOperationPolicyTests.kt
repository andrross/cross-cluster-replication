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

import org.opensearch.common.lucene.Lucene
import org.opensearch.index.VersionType
import org.opensearch.index.engine.Engine
import org.opensearch.index.engine.EngineTestCase
import org.opensearch.index.engine.InternalEngine
import org.opensearch.index.seqno.SequenceNumbers
import org.opensearch.index.store.Store
import org.opensearch.index.translog.Translog
import java.nio.file.Path

/**
 * Verifies the follower indexing behavior that [FollowerPrimaryOperationPolicy] carries, driving a
 * plain [InternalEngine] configured with the policy. This replaces the old `ReplicationEngineTests`,
 * which exercised the same behavior through an `InternalEngine` subclass.
 */
class FollowerPrimaryOperationPolicyTests : EngineTestCase() {

    private lateinit var followerEngine: InternalEngine
    private lateinit var followerStore: Store
    private lateinit var followerTranslogDir: Path

    override fun setUp() {
        super.setUp()
        followerStore = createStore()
        Lucene.cleanLuceneIndex(followerStore.directory())
        followerTranslogDir = createTempDir("translog-replication")
        val config = config(engine.config(), followerStore, followerTranslogDir, engine.config().tombstoneDocSupplier)
            .toBuilder()
            .primaryOperationPolicy(FollowerPrimaryOperationPolicy)
            .build()
        followerStore.createEmpty(config.indexSettings.indexVersionCreated.luceneVersion)
        val translogUuid = Translog.createEmptyTranslog(
            followerTranslogDir,
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            primaryTerm.get()
        )
        followerStore.associateIndexWithNewTranslog(translogUuid)
        followerEngine = InternalEngine(config)
        followerEngine.translogManager().recoverFromTranslog(
            createTranslogHandler(config.indexSettings, followerEngine),
            followerEngine.processedLocalCheckpoint,
            Long.MAX_VALUE
        )
    }

    override fun tearDown() {
        followerEngine.close()
        followerStore.close()
        super.tearDown()
    }

    /** A leader-replayed op: PRIMARY origin, but carrying the leader's sequence number. */
    private fun leaderOp(id: String, seqNo: Long, version: Long = 1L): Engine.Index {
        val doc = createParsedDoc(id, null)
        return Engine.Index(
            newUid(doc), doc, seqNo, primaryTerm.get(), version, VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1L, false,
            SequenceNumbers.UNASSIGNED_SEQ_NO, 0L
        )
    }

    fun testLeaderAssignedSeqNoIsUsedVerbatim() {
        val result = followerEngine.index(leaderOp("1", 7L))
        assertEquals(Engine.Result.Type.SUCCESS, result.resultType)
        assertEquals("the leader's seqNo must be used, not a locally generated one", 7L, result.seqNo)
    }

    /**
     * The common replication case: the leader updates a document the follower already has. The
     * update must replace the existing document rather than being planned as an append-only insert,
     * which would leave two Lucene documents for the same `_id`.
     *
     * Correct planning depends on the shard's max_seq_no_of_updates_or_deletes having been advanced
     * to the leader's value before the operation is applied, which is what
     * `TransportReplayChangesAction` does on the primary. This test performs that advance the same
     * way; without it the replica planning path takes the append-only optimization.
     */
    fun testLeaderUpdateOfExistingDocReplacesIt() {
        followerEngine.index(leaderOp("1", 0L, version = 1L))

        // as TransportReplayChangesAction does before applying each replayed op
        followerEngine.advanceMaxSeqNoOfUpdatesOrDeletes(1L)
        val update = followerEngine.index(leaderOp("1", 1L, version = 2L))

        assertEquals(Engine.Result.Type.SUCCESS, update.resultType)
        assertEquals(1L, update.seqNo)
        assertEquals("the leader's version must be applied verbatim", 2L, update.version)

        val docIds = getDocIds(followerEngine, true)
        assertEquals("the update must replace the document, not duplicate it: $docIds", 1, docIds.size)
        assertEquals("1", docIds[0].id)
        assertEquals(1L, docIds[0].seqNo)
    }

    /**
     * A leader delete of an existing document, planned with replica semantics under the same
     * precondition as an update.
     */
    fun testLeaderDeleteOfExistingDoc() {
        followerEngine.index(leaderOp("1", 0L, version = 1L))

        followerEngine.advanceMaxSeqNoOfUpdatesOrDeletes(1L)
        val doc = createParsedDoc("1", null)
        val delete = Engine.Delete(
            "1", newUid(doc), 1L, primaryTerm.get(), 2L, VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY, System.nanoTime(),
            SequenceNumbers.UNASSIGNED_SEQ_NO, 0L, null
        )
        val result = followerEngine.delete(delete)

        assertEquals(Engine.Result.Type.SUCCESS, result.resultType)
        assertEquals("the leader's seqNo must be used, not a locally generated one", 1L, result.seqNo)
        assertEquals(emptyList<Any>(), getDocIds(followerEngine, true))
    }

    fun testFillSeqNoGapsIsNoOpEvenWithGap() {
        followerEngine.index(leaderOp("0", 0L))
        // skip seqNo 1 to create a gap
        followerEngine.index(leaderOp("2", 2L))

        // Verify the gap exists: local checkpoint should be 0 (seqNo 1 is missing)
        assertEquals(
            "Local checkpoint should be 0 because seqNo 1 is missing",
            0L, followerEngine.processedLocalCheckpoint
        )

        // On a follower this MUST be a no-op — the leader owns the seqNo space
        val filled = followerEngine.fillSeqNoGaps(primaryTerm.get())
        assertEquals("fillSeqNoGaps() must return 0 (no-op) under the follower strategy", 0, filled)

        // Verify the gap is still there (no no-ops were written to fill seqNo 1)
        assertEquals(
            "Local checkpoint should still be 0 - the gap must NOT be filled",
            0L, followerEngine.processedLocalCheckpoint
        )
    }

    fun testBaselineInternalEngineFillsGaps() {
        // Create the same gap scenario on the standard engine
        val doc0 = createParsedDoc("baseline-0", null)
        engine.index(indexForDoc(doc0))

        // Generate a seqNo without indexing to create a gap
        generateNewSeqNo(engine)

        // Index another doc (takes the next seqNo after the gap)
        val doc2 = createParsedDoc("baseline-2", null)
        engine.index(indexForDoc(doc2))

        // The engine's local checkpoint should have a gap
        val lcpBefore = engine.processedLocalCheckpoint

        // fillSeqNoGaps on a default-strategy InternalEngine should fill the gap
        val filled = engine.fillSeqNoGaps(primaryTerm.get())
        assertTrue("InternalEngine.fillSeqNoGaps() should fill the gap (filled=$filled)", filled > 0)

        // After filling, the local checkpoint advances
        assertTrue(
            "Local checkpoint should advance after filling gaps",
            engine.processedLocalCheckpoint > lcpBefore
        )
    }

    /**
     * The leader has already resolved version conflicts, so a compare-and-swap precondition that
     * would fail on an ordinary primary must not be re-evaluated on the follower.
     */
    fun testCompareAndSwapChecksAreSkipped() {
        val doc = createParsedDoc("1", null)
        val opWithUnsatisfiableCas = Engine.Index(
            newUid(doc), doc, 0L, primaryTerm.get(), 1L, VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY, System.nanoTime(), -1L, false,
            99L, 1L
        )
        val result = followerEngine.index(opWithUnsatisfiableCas)
        assertEquals(Engine.Result.Type.SUCCESS, result.resultType)
        assertEquals(0L, result.seqNo)
    }
}
