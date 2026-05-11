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

import org.opensearch.cluster.ClusterState

/**
 * A handler for one metadata category (component templates, ingest pipelines, indices, etc.).
 *
 * Contract: upsert() and delete() are idempotent. equal() compares two instances of the
 * category's object type for "meaningful equality" — handlers may ignore sub-fields that don't
 * represent a difference worth applying (e.g., a version counter inside the object itself).
 *
 * The handler is responsible for its own object type `T`. The apply pipeline only sees the
 * handler through this interface.
 */
interface CategoryHandler<T : Any> {

    /** Stable identifier used in the registry and in metrics. */
    fun category(): String

    /** Extract this category's objects from a cluster state (local or primary's response). */
    fun extract(clusterState: ClusterState): Map<String, T>

    /**
     * Meaningful-equality predicate. Default is reference/structural `==`; handlers can
     * override to ignore sub-fields that don't represent a real change.
     */
    fun equal(local: T, primary: T): Boolean = local == primary

    /**
     * Apply an upsert for `name`, using `primary` as the source of truth. Idempotent.
     */
    fun upsert(name: String, primary: T): ApplyResult

    /**
     * Delete `name` from this cluster. Idempotent — delete of a missing object is a success.
     */
    fun delete(name: String): ApplyResult
}

/**
 * Outcome of a single upsert/delete on the secondary.
 *
 * Permanent failures are quarantined and the pipeline moves on (per the tenet). Transient
 * failures may be retried within the same apply cycle; if still failing, they are quarantined
 * and the pipeline still advances last_applied_metadata_version.
 */
sealed class ApplyResult {
    object Success : ApplyResult()
    data class PermanentFailure(val reason: String, val cause: Throwable? = null) : ApplyResult()
    data class TransientFailure(val reason: String, val cause: Throwable? = null) : ApplyResult()
}
