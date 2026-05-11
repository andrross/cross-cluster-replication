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

package org.opensearch.replication.v2.metadata.handlers

import org.apache.logging.log4j.LogManager
import org.opensearch.cluster.ClusterState
import org.opensearch.replication.v2.metadata.ApplyResult
import org.opensearch.replication.v2.metadata.CategoryHandler

/**
 * Base class handlers that extract via a supplied lambda and log
 * what they would apply rather than actually writing to local state.
 *
 * The concrete handlers land here so the full pipeline (diff, per-category ordering, quarantine
 * counters) can be exercised end-to-end while the real per-category writes are built out.
 */
abstract class LoggingCategoryHandler<T : Any>(
    private val name: String,
    private val extractor: (ClusterState) -> Map<String, T>
) : CategoryHandler<T> {

    private val log = LogManager.getLogger(javaClass)

    override fun category(): String = name

    override fun extract(clusterState: ClusterState): Map<String, T> = extractor(clusterState)

    override fun upsert(name: String, primary: T): ApplyResult {
        log.info("v2[{}] upsert {} (logging-only stub)", this.name, name)
        return ApplyResult.Success
    }

    override fun delete(name: String): ApplyResult {
        log.info("v2[{}] delete {} (logging-only stub)", this.name, name)
        return ApplyResult.Success
    }
}
