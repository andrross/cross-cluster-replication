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

import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.replication.ReplicationPlugin

/**
 * Single choke point for "is this index in scope for full-cluster replication?"
 *
 * v1 rule: every user index. No user-configurable pattern list, no exclude pattern list.
 * Concretely we exclude:
 *   - `.`-prefixed names (convention for OpenSearch-internal indices)
 *   - indices registered via SystemIndexPlugin (IndexMetadata.isSystem() == true)
 *   - hidden indices (IndexMetadata.INDEX_HIDDEN_SETTING)
 *
 * Data-stream backing indices are *included* — they carry user data and the data_streams
 * metadata category will propagate the logical stream separately.
 *
 * A future feature layering user-specified exclude patterns on top adds one condition to
 * isReplicable(). No call-site refactor needed; callers see a single predicate.
 */
object ReplicationScope {

    fun isReplicable(indexMetadata: IndexMetadata): Boolean {
        val name = indexMetadata.index.name
        if (name.startsWith(".")) return false
        if (indexMetadata.isSystem) return false
        if (IndexMetadata.INDEX_HIDDEN_SETTING.get(indexMetadata.settings)) return false
        return true
    }

    /**
     * Has this index been marked as a follower index on this cluster? True iff
     * REPLICATED_INDEX_SETTING is populated — set by BootstrapOrchestrator on restore.
     */
    fun isReplicatedFollower(indexMetadata: IndexMetadata): Boolean {
        val v = indexMetadata.settings.get(ReplicationPlugin.REPLICATED_INDEX_SETTING.key)
        return !v.isNullOrEmpty()
    }
}
