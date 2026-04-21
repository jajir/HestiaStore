package org.hestiastore.index.segmentindex.core.lifecycle;

/**
 * Close-time access to runtime collaborators that must be drained, flushed, or
 * shut down in a strict order.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface IndexCloseAccess<K, V> {

    void awaitBackgroundSplitsExhausted();

    void flushStableSegmentsWithSplitSchedulingPaused();

    void closeSegmentRegistry();

    void flushKeyToSegmentMap();

    void checkpointWal();

    void closeWalRuntime();
}
