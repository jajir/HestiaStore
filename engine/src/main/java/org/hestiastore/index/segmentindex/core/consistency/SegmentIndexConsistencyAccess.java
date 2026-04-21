package org.hestiastore.index.segmentindex.core.consistency;

import java.util.function.Predicate;

import org.hestiastore.index.segment.SegmentId;

/**
 * Capability view exposing only runtime operations needed by consistency and
 * recovery orchestration.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentIndexConsistencyAccess<K, V> {

    void validateUniqueSegmentIds();

    void checkAndRepairConsistency(Predicate<SegmentId> segmentFilter);

    void cleanupOrphanedSegmentDirectories();

    void scheduleBackgroundSplitScan();

    boolean hasSegmentLockFile(SegmentId segmentId);
}
