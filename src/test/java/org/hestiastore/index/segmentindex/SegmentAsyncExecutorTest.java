package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

class SegmentAsyncExecutorTest {

    @Test
    void usesMinimumQueueCapacity() {
        try (SegmentAsyncExecutor executor = new SegmentAsyncExecutor(1,
                "segment-async")) {
            assertEquals(64, executor.getQueueCapacity());
            assertNotNull(executor.getExecutor());
        }
    }

    @Test
    void scalesQueueCapacityWithThreads() {
        try (SegmentAsyncExecutor executor = new SegmentAsyncExecutor(2,
                "segment-async")) {
            assertEquals(128, executor.getQueueCapacity());
        }
    }
}
