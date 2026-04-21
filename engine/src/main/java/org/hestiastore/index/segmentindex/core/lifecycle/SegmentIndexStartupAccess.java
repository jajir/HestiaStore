package org.hestiastore.index.segmentindex.core.lifecycle;

/**
 * Capability view exposing only the startup actions needed during index open.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentIndexStartupAccess<K, V> {

    void recoverFromWal();

    void cleanupOrphanedSegmentDirectories();

    void scheduleBackgroundSplitScan();
}
