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

import org.opensearch.action.ActionType

class FlushAndGetHandoffCheckpointShardAction private constructor() :
    ActionType<FlushAndGetHandoffCheckpointShardResponse>(NAME, ::FlushAndGetHandoffCheckpointShardResponse) {
    companion object {
        const val NAME = "indices:admin/plugins/replication/switchover/flush_and_get_handoff_checkpoint[s]"
        val INSTANCE: FlushAndGetHandoffCheckpointShardAction = FlushAndGetHandoffCheckpointShardAction()
    }
}
