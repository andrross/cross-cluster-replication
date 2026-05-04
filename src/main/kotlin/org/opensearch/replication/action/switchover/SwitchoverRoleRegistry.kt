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

import org.opensearch.core.index.shard.ShardId
import java.util.concurrent.ConcurrentHashMap

/**
 * Per-shard role override consulted by the plugin's engine factory during switchover.
 *
 * Background: the engine factory decision is made once, at shard construction, based on
 * whether the index has REPLICATED_INDEX_SETTING. For an in-place switchover we need the
 * factory to return a different engine type *after* the shard is already running, so
 * IndexShard.resetToWriteableEngine can swap in a fresh engine of the new role.
 *
 * Usage: the promote action sets role=LEADER for a shard *before* calling
 * resetToWriteableEngine. The engine factory reads from this registry on each
 * construction; the next engine built for that shard will be an InternalEngine rather
 * than a ReplicationEngine, enabling the shard to admit writes directly.
 *
 * This is in-memory only. Role durability across node restart is a separate concern —
 * see commit user data notes in switchover-implementation-plan.md. For phase A we accept
 * that a node restart mid-switchover drops the override; the shard recovers into its
 * original engine, which is safe because writes are still fenced at the cluster-state
 * block level.
 */
enum class SwitchoverRole { LEADER, FOLLOWER }

class SwitchoverRoleRegistry {
    private val overrides = ConcurrentHashMap<ShardId, SwitchoverRole>()

    fun get(shardId: ShardId): SwitchoverRole? = overrides[shardId]

    fun set(shardId: ShardId, role: SwitchoverRole) {
        overrides[shardId] = role
    }

    fun clear(shardId: ShardId) {
        overrides.remove(shardId)
    }
}
