package org.hestiastore.index.segmentregistry;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.segment.SegmentFullWriterTx;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentRuntimeLimits;

/**
 * Main public entry point for segment registry operations and related runtime
 * views.
 * <p>
 * Contract source of truth is {@code docs/architecture/registry.md}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentRegistry<K, V> {

    /**
     * Creates a builder for registry instances.
     *
     * @param <M> key type
     * @param <N> value type
     * @return registry builder
     */
    static <M, N> SegmentRegistryBuilder<M, N> builder() {
        return new SegmentRegistryBuilder<>();
    }

    /**
     * Returns the materialization view backed by the same registry runtime.
     *
     * @return registry materialization view
     */
    Materialization<K, V> materialization();

    /**
     * Returns the runtime tuning view backed by the same registry runtime.
     *
     * @return registry runtime view
     */
    Runtime<K, V> runtime();

    /**
     * Returns the segment handle for the provided id, waiting until the
     * registry can load it or a terminal failure is reached.
     *
     * @param segmentId segment id to load
     * @return loaded segment handle
     */
    SegmentHandle<K, V> loadSegment(SegmentId segmentId);

    /**
     * Performs a bounded fail-fast attempt to load the requested segment.
     *
     * @param segmentId segment id to load
     * @return loaded segment when immediately available, otherwise empty
     */
    Optional<SegmentHandle<K, V>> tryGetSegment(SegmentId segmentId);

    /**
     * Creates and registers a new segment, waiting until the segment becomes
     * available or a terminal failure is reached.
     *
     * @return created segment handle
     */
    SegmentHandle<K, V> createSegment();

    /**
     * Removes a segment from the registry, waiting until the segment is
     * deleted or a terminal failure is reached.
     *
     * @param segmentId segment id to remove
     */
    void deleteSegment(SegmentId segmentId);

    /**
     * Performs a bounded fail-fast attempt to delete the requested segment.
     *
     * @param segmentId segment id to remove
     * @return true when the segment was deleted or already closed, false when
     *         it remained busy
     */
    boolean deleteSegmentIfAvailable(SegmentId segmentId);

    /**
     * Returns immutable cache counters snapshot.
     *
     * @return registry cache metrics snapshot
     */
    SegmentRegistryCacheStats metricsSnapshot();

    /**
     * Updates registry cache limit at runtime.
     *
     * @param newLimit new cache limit
     * @return true when update was applied
     */
    boolean updateCacheLimit(int newLimit);

    /**
     * Closes the registry, releasing cached segments and executors.
     * <p>
     * Close is idempotent and maps the gate to {@code CLOSED} unless the gate
     * is already terminal {@code ERROR}.
     *
     */
    void close();

    /**
     * Materialization helpers exposed by the registry package.
     *
     * @param <K> key type
     * @param <V> value type
     */
    interface Materialization<K, V> {

        /**
         * Allocates the next segment id for offline materialization.
         *
         * @return next segment id
         */
        SegmentId nextSegmentId();

        /**
         * Opens a synchronous bulk writer transaction for the provided segment
         * id.
         *
         * @param segmentId segment id to materialize
         * @return full writer transaction for building the segment files
         */
        SegmentFullWriterTx<K, V> openWriterTx(SegmentId segmentId);
    }

    /**
     * Runtime tuning helpers exposed by the registry package.
     */
    interface Runtime<K, V> {

        /**
         * Updates runtime-only limits used for future segment materialization.
         *
         * @param runtimeLimits validated segment runtime limits
         */
        void updateRuntimeLimits(SegmentRuntimeLimits runtimeLimits);

        /**
         * Returns currently loaded segment instances.
         *
         * @return loaded segment snapshot
         */
        List<SegmentHandle<K, V>> loadedSegmentsSnapshot();
    }
}
