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

package org.opensearch.replication.v2.action.status

import org.opensearch.action.ActionType

class GetReplicationStatusAction private constructor() :
    ActionType<GetReplicationStatusResponse>(NAME, ::GetReplicationStatusResponse) {
    companion object {
        const val NAME = "cluster:admin/plugins/replication/status/get"
        val INSTANCE: GetReplicationStatusAction = GetReplicationStatusAction()
    }
}
