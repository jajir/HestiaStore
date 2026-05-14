package org.hestiastore.index.segmentindex.core.maintenance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MaintenanceStatsRecorderTest {

    @Test
    void statsExposeRecordedCountersAndLatencies() {
        final MaintenanceStatsRecorder recorder =
                new MaintenanceStatsRecorder();

        recorder.recordFlushRequest();
        recorder.recordCompactRequest();
        recorder.recordFlushBusyRetry();
        recorder.recordCompactBusyRetry();
        recorder.recordFlushAcceptedToReadyNanos(10_000L);
        recorder.recordCompactAcceptedToReadyNanos(11_000L);

        final MaintenanceStats stats = recorder.statsSnapshot();
        assertEquals(1L, stats.getFlushRequestCount());
        assertEquals(1L, stats.getCompactRequestCount());
        assertEquals(1L, stats.getFlushBusyRetryCount());
        assertEquals(1L, stats.getCompactBusyRetryCount());
        assertTrue(stats.getFlushAcceptedToReadyP95Micros() >= 10L);
        assertTrue(stats.getCompactAcceptedToReadyP95Micros() >= 11L);
    }
}
