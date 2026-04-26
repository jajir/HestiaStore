package org.hestiastore.index.segmentindex.core.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class StatsLatencySetTest {

    @Test
    void latenciesTrackRecordedPercentiles() {
        final StatsLatencySet latencies = new StatsLatencySet();

        latencies.recordReadLatencyNanos(2_000L);
        latencies.recordWriteLatencyNanos(3_000L);
        latencies.recordDrainLatencyNanos(4_000L);
        latencies.recordSplitTaskStartDelayNanos(6_000L);
        latencies.recordSplitTaskRunLatencyNanos(7_000L);
        latencies.recordDrainTaskStartDelayNanos(8_000L);
        latencies.recordDrainTaskRunLatencyNanos(9_000L);
        latencies.recordFlushAcceptedToReadyNanos(10_000L);
        latencies.recordCompactAcceptedToReadyNanos(11_000L);

        assertTrue(latencies.getReadLatencyP50Micros() >= 2L);
        assertTrue(latencies.getReadLatencyP95Micros() >= 2L);
        assertTrue(latencies.getReadLatencyP99Micros() >= 2L);
        assertTrue(latencies.getWriteLatencyP50Micros() >= 3L);
        assertTrue(latencies.getWriteLatencyP95Micros() >= 3L);
        assertTrue(latencies.getWriteLatencyP99Micros() >= 3L);
        assertTrue(latencies.getDrainLatencyP95Micros() >= 4L);
        assertTrue(latencies.getSplitTaskStartDelayP95Micros() >= 6L);
        assertTrue(latencies.getSplitTaskRunLatencyP95Micros() >= 7L);
        assertTrue(latencies.getDrainTaskStartDelayP95Micros() >= 8L);
        assertTrue(latencies.getDrainTaskRunLatencyP95Micros() >= 9L);
        assertTrue(latencies.getFlushAcceptedToReadyP95Micros() >= 10L);
        assertTrue(latencies.getCompactAcceptedToReadyP95Micros() >= 11L);
    }

    @Test
    void latenciesDefaultToZeroWithoutSamples() {
        final StatsLatencySet latencies = new StatsLatencySet();

        assertEquals(0L, latencies.getReadLatencyP50Micros());
        assertEquals(0L, latencies.getWriteLatencyP95Micros());
        assertEquals(0L, latencies.getCompactAcceptedToReadyP95Micros());
    }
}
