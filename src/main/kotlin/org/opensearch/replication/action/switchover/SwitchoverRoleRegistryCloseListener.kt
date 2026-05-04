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

import org.opensearch.common.settings.Settings
import org.opensearch.core.index.shard.ShardId
import org.opensearch.index.shard.IndexEventListener
import org.opensearch.index.shard.IndexShard

/**
 * Clears the [SwitchoverRoleRegistry] override for a shard when the shard closes.
 *
 * The registry override is how promote installs its role decision (LEADER) for the engine
 * factory to consult. Once a shard has been promoted, the override persists in memory. If
 * the shard later closes — because of demote, node restart, shard reallocation, or an
 * explicit index close — the override must be cleared so the shard's next engine
 * construction falls back to the normal factory decision (which picks ReplicationEngine
 * based on REPLICATED_INDEX_SETTING).
 *
 * This is specifically what unblocks demote-after-promote: demote closes the index,
 * which closes each primary shard, which fires this listener and clears the LEADER
 * override. The subsequent reopen then constructs a ReplicationEngine as intended.
 *
 * On any close the override is cleared, which means a node-restart recovery drops the
 * promote and the shard comes back as a follower (still fenced at the cluster-state
 * level). Phase A accepts this; a later slice will persist the role durably in Lucene
 * commit user data.
 */
class SwitchoverRoleRegistryCloseListener(
    private val registry: SwitchoverRoleRegistry
) : IndexEventListener {

    override fun beforeIndexShardClosed(shardId: ShardId, indexShard: IndexShard?, indexSettings: Settings) {
        registry.clear(shardId)
    }
}
