package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.segment.SegmentId;

/**
 * Public split-management boundary exposed to the rest of the runtime.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SplitService<K, V> {

    /**
     * Requests a targeted split eligibility check for a mapped segment.
     *
     * @param segmentId mapped segment id
     */
    void requestSegmentCheck(SegmentId segmentId);

    /**
     * Requests a full split-policy scan over currently mapped segments.
     */
    void requestFullScan();

    /**
     * Waits until split policy work and in-flight splits have drained or the
     * timeout expires.
     *
     * @param timeoutMillis wait timeout in milliseconds
     */
    void awaitIdle(long timeoutMillis);

    /**
     * @return immutable split runtime snapshot
     */
    SplitRuntimeSnapshot runtimeSnapshot();
}
