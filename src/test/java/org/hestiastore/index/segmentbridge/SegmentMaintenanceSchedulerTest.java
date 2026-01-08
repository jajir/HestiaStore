package org.hestiastore.index.segmentbridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.segment.Segment;
import org.junit.jupiter.api.Test;

class SegmentMaintenanceSchedulerTest {

    @Test
    void scheduleAfterWrite_runsConfiguredTasks() {
        final SegmentMaintenancePolicy<Integer, String> policy = segment -> SegmentMaintenanceDecision
                .flushAndCompact();
        final SegmentMaintenanceScheduler<Integer, String> scheduler = new SegmentMaintenanceScheduler<>(
                Runnable::run, policy);
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = (Segment<Integer, String>) mock(
                Segment.class);
        final AtomicInteger flushCount = new AtomicInteger();
        final AtomicInteger compactCount = new AtomicInteger();

        scheduler.scheduleAfterWrite(segment, flushCount::incrementAndGet,
                compactCount::incrementAndGet);

        assertEquals(1, flushCount.get());
        assertEquals(1, compactCount.get());
    }
}
