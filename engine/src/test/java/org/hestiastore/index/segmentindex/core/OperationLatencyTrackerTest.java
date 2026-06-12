package org.hestiastore.index.segmentindex.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class OperationLatencyTrackerTest {

    @Test
    void tracksRecordedPercentiles() {
        final OperationLatencyTracker tracker = new OperationLatencyTracker();

        tracker.recordNanos(2_000L);
        tracker.recordNanos(3_000L);

        assertTrue(tracker.percentileMicros(0.50D) >= 2L);
        assertTrue(tracker.percentileMicros(0.95D) >= 3L);
        assertTrue(tracker.percentileMicros(0.99D) >= 3L);
    }

    @Test
    void defaultsToZeroWithoutSamples() {
        final OperationLatencyTracker tracker = new OperationLatencyTracker();

        assertEquals(0L, tracker.percentileMicros(0.95D));
    }
}
