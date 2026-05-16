package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.segment.SegmentId;

/**
 * Coordinates split execution and lease-backed split publishing after the
 * policy layer has already accepted a candidate.
 *
 * @param <K> key type
 * @param <V> value type
 */
interface SplitExecutionCoordinator<K, V> {

    /**
     * Schedules split execution for a candidate already accepted by the policy
     * layer.
     *
     * @param segmentId        accepted split candidate id
     * @param splitThreshold   active split threshold
     * @param observedKeyCount key count observed by policy evaluation
     * @return {@code true} when split work was scheduled
     */
    boolean scheduleEligibleSplit(SegmentId segmentId,
            long splitThreshold, long observedKeyCount);

    /**
     * Waits until in-flight splits finish or the timeout expires.
     *
     * @param timeoutMillis wait timeout in milliseconds
     */
    void awaitSplitsIdle(long timeoutMillis);

    /**
     * @return number of scheduled or running split tasks
     */
    int splitInFlightCount();

    /**
     * @param segmentId segment id
     * @return {@code true} when the segment currently has a scheduled or
     *         running split
     */
    boolean isSplitBlocked(SegmentId segmentId);

    /**
     * @return number of blocked segments with scheduled or running splits
     */
    int splitBlockedCount();

}
