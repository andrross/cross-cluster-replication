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

import org.opensearch.cluster.block.ClusterBlock
import org.opensearch.cluster.block.ClusterBlockLevel
import org.opensearch.core.rest.RestStatus
import java.util.EnumSet

/**
 * Cluster block installed on the leader index during a managed switchover to fence writes
 * while the follower drains to the sealed seqno.
 *
 * Distinct from INDEX_REPLICATION_BLOCK (which is used on follower indices during ongoing
 * replication): the block id is different, the wording is different, and the semantics are
 * different — fence means "writes are being drained as part of a role swap," whereas the
 * replication block means "this index is a follower and should not be modified directly."
 */
val INDEX_SWITCHOVER_FENCE_BLOCK = ClusterBlock(
    1001,
    "index fenced for cross-cluster-replication switchover",
    false,
    false,
    false,
    RestStatus.FORBIDDEN,
    EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE))
