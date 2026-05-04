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

class VerifyCaughtUpShardAction private constructor() :
    ActionType<VerifyCaughtUpShardResponse>(NAME, ::VerifyCaughtUpShardResponse) {
    companion object {
        const val NAME = "indices:admin/plugins/replication/switchover/verify_caught_up[s]"
        val INSTANCE: VerifyCaughtUpShardAction = VerifyCaughtUpShardAction()
    }
}
