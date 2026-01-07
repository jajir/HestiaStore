package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

class SegmentWriteLockSupportTest {

    @Test
    void executeWithWriteLock_returnsTaskResult() {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> delegate = (Segment<Integer, String>) mock(
                Segment.class);
        final SegmentWriteLockSupport<Integer, String> adapter = new SegmentImplSynchronizationAdapter<>(
                delegate);

        final String result = adapter.executeWithWriteLock(() -> "value");

        assertEquals("value", result);
    }

    @Test
    void executeWithMaintenanceWriteLock_returnsTaskResult() {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> delegate = (Segment<Integer, String>) mock(
                Segment.class);
        final SegmentWriteLockSupport<Integer, String> adapter = new SegmentImplSynchronizationAdapter<>(
                delegate);

        final String result = adapter
                .executeWithMaintenanceWriteLock(() -> "maintenance");

        assertEquals("maintenance", result);
    }
}
