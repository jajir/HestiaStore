package org.hestiastore.index.segmentasync;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.junit.jupiter.api.Test;

class SegmentMaintenanceQueueTest {

    @Test
    void submitMaintenanceTask_executesRunnable() throws Exception {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> delegate = (Segment<Integer, String>) mock(
                Segment.class);
        final SegmentMaintenanceQueue queue = new SegmentAsyncAdapter<>(
                delegate, Runnable::run, SegmentMaintenancePolicy.none());
        final AtomicInteger executed = new AtomicInteger();

        final SegmentResult<Void> result = queue
                .submitMaintenanceTask(SegmentMaintenanceTask.SPLIT,
                        executed::incrementAndGet)
                .toCompletableFuture().get(1, TimeUnit.SECONDS);

        assertEquals(1, executed.get());
        assertEquals(SegmentResultStatus.OK, result.getStatus());
    }
}
