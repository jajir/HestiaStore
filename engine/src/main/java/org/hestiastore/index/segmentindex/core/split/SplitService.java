package org.hestiastore.index.segmentindex.core.split;

import java.time.Duration;

import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentindex.core.maintenance.SplitMaintenanceSynchronization;
import org.hestiastore.index.segmentindex.core.routing.SplitAdmissionAccess;

/**
 * Public split-management boundary exposed to the rest of the runtime.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SplitService<K, V> extends AutoCloseable {

    /**
     * Creates a builder for the default split runtime service.
     *
     * @param <M> key type
     * @param <N> value type
     * @return split service builder
     */
    static <M, N> SplitServiceBuilder<M, N> builder() {
        return new SplitServiceBuilder<>();
    }

    /**
     * Hints that the provided mapped segment may now be eligible for split.
     *
     * @param segmentId mapped segment id
     */
    void hintSplitCandidate(SegmentId segmentId);

    /**
     * Waits until split policy work and in-flight splits have drained or the
     * timeout expires.
     *
     * @param timeout wait timeout
     */
    void awaitQuiescence(Duration timeout);

    /**
     * Returns the routing-facing split admission view backed by the same split
     * runtime.
     *
     * @return split admission view
     */
    SplitAdmissionAccess<K, V> splitAdmission();

    /**
     * Returns the maintenance-facing synchronization view backed by the same
     * split runtime.
     *
     * @return maintenance-facing synchronization view
     */
    SplitMaintenanceSynchronization<K, V> splitMaintenance();

    /**
     * Returns the metrics-facing split runtime view backed by the same split
     * runtime.
     *
     * @return split metrics view
     */
    SplitMetricsView splitMetricsView();

    /**
     * Closes the managed split runtime. Shared executors are owned by the
     * surrounding runtime and are not shut down by this call.
     */
    @Override
    void close();
}
