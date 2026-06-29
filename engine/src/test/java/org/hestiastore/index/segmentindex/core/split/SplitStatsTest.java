package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SplitStatsTest {

    @Test
    void exposesProvidedCounterValues() {
        final SplitStats snapshot = new SplitStats(3L, 5, 7, 11L, 13L);

        assertEquals(3L, snapshot.splitScheduleCount());
        assertEquals(5, snapshot.splitInFlightCount());
        assertEquals(7, snapshot.splitBlockedCount());
        assertEquals(11L, snapshot.splitTaskStartDelayP95Micros());
        assertEquals(13L, snapshot.splitTaskRunLatencyP95Micros());
    }

    @Test
    void recorderSnapshotsRecordedCountersAndLatencies() {
        final SplitStatsRecorder recorder = new SplitStatsRecorder();

        recorder.recordSplitScheduled();
        recorder.recordSplitTaskStartDelayNanos(5_000L);
        recorder.recordSplitTaskRunLatencyNanos(9_000L);

        final SplitStats snapshot = recorder.statsSnapshot(2, 1);
        assertEquals(1L, snapshot.splitScheduleCount());
        assertEquals(2, snapshot.splitInFlightCount());
        assertEquals(1, snapshot.splitBlockedCount());
        assertEquals(5L, snapshot.splitTaskStartDelayP95Micros());
        assertEquals(9L, snapshot.splitTaskRunLatencyP95Micros());
    }
}
