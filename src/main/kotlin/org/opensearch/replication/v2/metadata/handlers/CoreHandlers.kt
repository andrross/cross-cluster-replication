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

import org.opensearch.cluster.metadata.DataStream
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.replication.v2.metadata.CategoryHandler
import org.opensearch.replication.v2.metadata.CategoryHandlerRegistry
import org.opensearch.script.ScriptMetadata
import org.opensearch.search.pipeline.SearchPipelineMetadata
import org.opensearch.transport.client.Client

/**
 * Core handlers.
 *
 * For now these are logging stubs — they produce a correct diff against local state and log
 * what would be applied. The actual cluster-state mutations land in later iterations when each
 * category gets a real apply implementation.
 *
 * Order below reflects the dependency graph from the design doc:
 *   component_templates → templates_v2 → templates (v1) → stored_scripts →
 *   ingest_pipelines → search_pipelines → persistent_settings → indices → data_streams
 */
object CoreHandlers {

    /** A pseudo-object capturing the cluster-wide persistent settings for diff purposes. */
    data class PersistentSettingsBag(val asMap: Map<String, String>)

    /** A pseudo-object capturing stored-scripts at the cluster level (ScriptMetadata exposes no public per-script map). */
    data class StoredScriptsBag(val fingerprint: String)

    fun defaultRegistry(client: Client): CategoryHandlerRegistry {
        val handlers: List<CategoryHandler<*>> = listOf(
            ComponentTemplateHandler(client),
            ComposableIndexTemplateHandler(client),
            LegacyIndexTemplateHandler(client),
            object : LoggingCategoryHandler<StoredScriptsBag>(
                "stored_scripts",
                { state ->
                    val script = state.metadata.custom<ScriptMetadata>(ScriptMetadata.TYPE)
                    if (script == null) emptyMap()
                    else mapOf("_all" to StoredScriptsBag(script.toString()))
                }
            ) {},
            IngestPipelineHandler(client),
            object : LoggingCategoryHandler<org.opensearch.search.pipeline.PipelineConfiguration>(
                "search_pipelines",
                { state ->
                    val sp = state.metadata.custom<SearchPipelineMetadata>(SearchPipelineMetadata.TYPE)
                    sp?.pipelines ?: emptyMap()
                }
            ) {},
            object : LoggingCategoryHandler<PersistentSettingsBag>(
                "persistent_settings",
                { state ->
                    val allowed = state.metadata.persistentSettings()
                        .keySet()
                        .associateWith { state.metadata.persistentSettings().get(it) ?: "" }
                    mapOf("_allowlisted" to PersistentSettingsBag(allowed))
                }
            ) {},
            object : LoggingCategoryHandler<IndexMetadata>(
                "indices",
                { it.metadata.indices() }
            ) {},
            object : LoggingCategoryHandler<DataStream>(
                "data_streams",
                { it.metadata.dataStreams() }
            ) {}
        )
        return CategoryHandlerRegistry(handlers)
    }
}
