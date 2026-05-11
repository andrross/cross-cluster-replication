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
import org.opensearch.ResourceNotFoundException
import org.opensearch.action.admin.indices.template.delete.DeleteComponentTemplateAction
import org.opensearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction
import org.opensearch.action.admin.indices.template.delete.DeleteIndexTemplateAction
import org.opensearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest
import org.opensearch.action.admin.indices.template.put.PutComponentTemplateAction
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateAction
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequest
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.AliasMetadata
import org.opensearch.cluster.metadata.ComponentTemplate
import org.opensearch.cluster.metadata.ComposableIndexTemplate
import org.opensearch.cluster.metadata.IndexTemplateMetadata
import org.opensearch.common.xcontent.XContentFactory
import org.opensearch.common.xcontent.XContentType
import org.opensearch.replication.v2.metadata.ApplyResult
import org.opensearch.replication.v2.metadata.CategoryHandler
import org.opensearch.transport.client.Client

/**
 * Handler for component_templates. Applies to local cluster via the normal Put/Delete
 * component-template transport actions.
 */
class ComponentTemplateHandler(private val client: Client) : CategoryHandler<ComponentTemplate> {

    private val log = LogManager.getLogger(javaClass)

    override fun category(): String = "component_templates"

    override fun extract(clusterState: ClusterState): Map<String, ComponentTemplate> =
        clusterState.metadata.componentTemplates()

    override fun upsert(name: String, primary: ComponentTemplate): ApplyResult {
        val req = PutComponentTemplateAction.Request(name).componentTemplate(primary)
        return try {
            val resp = client.execute(PutComponentTemplateAction.INSTANCE, req).actionGet()
            if (resp.isAcknowledged) {
                log.debug("component_templates: upsert {} applied", name)
                ApplyResult.Success
            } else {
                ApplyResult.TransientFailure("PutComponentTemplate not acknowledged for $name")
            }
        } catch (e: Exception) {
            ApplyResult.TransientFailure("PutComponentTemplate failed for $name: ${e.message}", e)
        }
    }

    override fun delete(name: String): ApplyResult {
        val req = DeleteComponentTemplateAction.Request(name)
        return try {
            val resp = client.execute(DeleteComponentTemplateAction.INSTANCE, req).actionGet()
            if (resp.isAcknowledged) {
                log.debug("component_templates: delete {} applied", name)
                ApplyResult.Success
            } else {
                ApplyResult.TransientFailure("DeleteComponentTemplate not acknowledged for $name")
            }
        } catch (e: Exception) {
            if (isNotFound(e)) {
                log.debug("component_templates: delete {} found nothing (ok)", name)
                return ApplyResult.Success
            }
            ApplyResult.TransientFailure("DeleteComponentTemplate failed for $name: ${e.message}", e)
        }
    }
}

/**
 * Handler for composable index templates (templates_v2).
 */
class ComposableIndexTemplateHandler(private val client: Client) : CategoryHandler<ComposableIndexTemplate> {

    private val log = LogManager.getLogger(javaClass)

    override fun category(): String = "templates_v2"

    override fun extract(clusterState: ClusterState): Map<String, ComposableIndexTemplate> =
        clusterState.metadata.templatesV2()

    override fun upsert(name: String, primary: ComposableIndexTemplate): ApplyResult {
        val req = PutComposableIndexTemplateAction.Request(name).indexTemplate(primary)
        return try {
            val resp = client.execute(PutComposableIndexTemplateAction.INSTANCE, req).actionGet()
            if (resp.isAcknowledged) {
                log.debug("templates_v2: upsert {} applied", name)
                ApplyResult.Success
            } else {
                ApplyResult.TransientFailure("PutComposableIndexTemplate not acknowledged for $name")
            }
        } catch (e: Exception) {
            ApplyResult.TransientFailure("PutComposableIndexTemplate failed for $name: ${e.message}", e)
        }
    }

    override fun delete(name: String): ApplyResult {
        val req = DeleteComposableIndexTemplateAction.Request(name)
        return try {
            val resp = client.execute(DeleteComposableIndexTemplateAction.INSTANCE, req).actionGet()
            if (resp.isAcknowledged) {
                log.debug("templates_v2: delete {} applied", name)
                ApplyResult.Success
            } else {
                ApplyResult.TransientFailure("DeleteComposableIndexTemplate not acknowledged for $name")
            }
        } catch (e: Exception) {
            if (isNotFound(e)) {
                log.debug("templates_v2: delete {} found nothing (ok)", name)
                return ApplyResult.Success
            }
            ApplyResult.TransientFailure("DeleteComposableIndexTemplate failed for $name: ${e.message}", e)
        }
    }
}

/**
 * Handler for v1 index templates. IndexTemplateMetadata is read from local cluster state but
 * written via PutIndexTemplateRequest, which requires each piece broken out.
 */
class LegacyIndexTemplateHandler(private val client: Client) : CategoryHandler<IndexTemplateMetadata> {

    private val log = LogManager.getLogger(javaClass)

    override fun category(): String = "templates"

    override fun extract(clusterState: ClusterState): Map<String, IndexTemplateMetadata> =
        clusterState.metadata.templates()

    override fun upsert(name: String, primary: IndexTemplateMetadata): ApplyResult {
        return try {
            val req = PutIndexTemplateRequest(name)
                .patterns(primary.patterns())
                .order(primary.order())
                .settings(primary.settings())
            primary.version()?.let { req.version(it) }
            primary.mappings()?.let { compressed ->
                req.mapping(compressed.string(), XContentType.JSON)
            }
            if (primary.aliases().isNotEmpty()) {
                req.aliases(aliasesToXContentBytes(primary.aliases()))
            }
            val resp = client.execute(PutIndexTemplateAction.INSTANCE, req).actionGet()
            if (resp.isAcknowledged) {
                log.debug("templates: upsert {} applied", name)
                ApplyResult.Success
            } else {
                ApplyResult.TransientFailure("PutIndexTemplate not acknowledged for $name")
            }
        } catch (e: Exception) {
            ApplyResult.TransientFailure("PutIndexTemplate failed for $name: ${e.message}", e)
        }
    }

    override fun delete(name: String): ApplyResult {
        val req = DeleteIndexTemplateRequest(name)
        return try {
            val resp = client.execute(DeleteIndexTemplateAction.INSTANCE, req).actionGet()
            if (resp.isAcknowledged) {
                log.debug("templates: delete {} applied", name)
                ApplyResult.Success
            } else {
                ApplyResult.TransientFailure("DeleteIndexTemplate not acknowledged for $name")
            }
        } catch (e: Exception) {
            if (isNotFound(e)) {
                log.debug("templates: delete {} found nothing (ok)", name)
                return ApplyResult.Success
            }
            ApplyResult.TransientFailure("DeleteIndexTemplate failed for $name: ${e.message}", e)
        }
    }

    /**
     * PutIndexTemplateRequest.aliases() takes a JSON BytesReference shaped like
     * `{"alias_name": {...alias settings...}, ...}`. Serialize the AliasMetadata map to that
     * shape using its existing XContent output.
     */
    private fun aliasesToXContentBytes(
        aliases: Map<String, AliasMetadata>
    ): org.opensearch.core.common.bytes.BytesReference {
        val builder = XContentFactory.jsonBuilder().startObject()
        for ((aliasName, aliasMeta) in aliases) {
            builder.startObject(aliasName)
            AliasMetadata.Builder.toXContent(aliasMeta, builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS)
            builder.endObject()
        }
        builder.endObject()
        return org.opensearch.core.common.bytes.BytesReference.bytes(builder)
    }
}

private fun isNotFound(e: Throwable): Boolean {
    var cur: Throwable? = e
    while (cur != null) {
        if (cur is ResourceNotFoundException) return true
        if (cur is org.opensearch.index.IndexNotFoundException) return true
        cur = cur.cause
    }
    return false
}
