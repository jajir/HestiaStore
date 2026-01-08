package org.hestiastore.index.segmentindex;

import org.hestiastore.index.segment.Segment;

/**
 * Decides when to schedule segment maintenance after writes.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentMaintenancePolicy<K, V> {

    SegmentMaintenanceDecision evaluateAfterWrite(Segment<K, V> segment);

    static <K, V> SegmentMaintenancePolicy<K, V> none() {
        return segment -> SegmentMaintenanceDecision.none();
    }
}
