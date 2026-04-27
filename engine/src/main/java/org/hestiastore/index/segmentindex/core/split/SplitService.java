package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.segment.SegmentId;

/**
 * Public split-management boundary exposed to the rest of the runtime.
 */
public interface SplitService extends AutoCloseable {

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
     * Requests a full split-policy scan regardless of current in-flight split
     * state.
     */
    void requestFullSplitScan();

    /**
     * Waits until split-policy work and in-flight splits are quiescent.
     */
    void awaitQuiescence();

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
