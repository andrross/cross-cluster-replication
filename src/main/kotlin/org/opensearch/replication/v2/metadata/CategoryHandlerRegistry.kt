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

package org.opensearch.replication.v2.metadata

/**
 * Ordered registry of category handlers. Order reflects the referential dependency graph from
 * the metadata-replication design:
 *
 *   component_templates → composable templates → v1 templates → stored scripts →
 *   ingest pipelines → search pipelines → persistent settings → indices → data streams
 *
 * Apply pipeline iterates in this order so dependencies land before dependents.
 */
class CategoryHandlerRegistry(handlers: List<CategoryHandler<*>>) {

    private val ordered: List<CategoryHandler<*>> = handlers.toList()
    private val byName: Map<String, CategoryHandler<*>> = ordered.associateBy { it.category() }

    init {
        require(byName.size == ordered.size) {
            "duplicate category handlers: ${ordered.map { it.category() }}"
        }
    }

    fun inApplyOrder(): List<CategoryHandler<*>> = ordered

    operator fun get(category: String): CategoryHandler<*>? = byName[category]
}
