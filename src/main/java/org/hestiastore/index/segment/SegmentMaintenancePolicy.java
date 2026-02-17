package org.hestiastore.index.segment;

/**
 * Decides when to schedule segment maintenance after writes.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface SegmentMaintenancePolicy<K, V> {

    /**
     * Evaluates the segment after a write to decide maintenance actions.
     *
     * @param segment segment to evaluate
     * @return maintenance decision for the segment
     */
    SegmentMaintenanceDecision evaluateAfterWrite(Segment<K, V> segment);

    /**
     * Returns a policy that never schedules maintenance.
     *
     * @param <K> key type
     * @param <V> value type
     * @return policy that always returns {@link SegmentMaintenanceDecision#none()}
     */
    static <K, V> SegmentMaintenancePolicy<K, V> none() {
        return segment -> SegmentMaintenanceDecision.none();
    }
}
