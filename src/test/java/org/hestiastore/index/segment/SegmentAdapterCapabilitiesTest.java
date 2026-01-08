package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hestiastore.index.segmentbridge.SegmentAsyncAdapter;
import org.hestiastore.index.segmentbridge.SegmentMaintenancePolicy;
import org.hestiastore.index.segmentbridge.SegmentMaintenanceQueue;
import org.junit.jupiter.api.Test;

class SegmentAdapterCapabilitiesTest {

    @Test
    void synchronizationAdapter_exposes_write_lock_support() {
        final Segment<Integer, String> segment = mock(Segment.class);
        final SegmentImplSynchronizationAdapter<Integer, String> adapter = new SegmentImplSynchronizationAdapter<>(
                segment);

        assertTrue(adapter instanceof SegmentWriteLockSupport);
    }

    @Test
    void asyncAdapter_exposes_maintenance_interfaces() {
        final Segment<Integer, String> segment = mock(Segment.class);
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final SegmentAsyncAdapter<Integer, String> adapter = new SegmentAsyncAdapter<>(
                    segment, executor, SegmentMaintenancePolicy.none());

            assertTrue(adapter instanceof SegmentMaintenanceQueue);
        } finally {
            executor.shutdownNow();
        }
    }
}
