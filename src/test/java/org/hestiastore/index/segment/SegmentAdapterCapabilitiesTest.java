package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hestiastore.index.segmentasync.SegmentAsyncAdapter;
import org.hestiastore.index.segmentasync.SegmentMaintenanceBlocking;
import org.hestiastore.index.segmentasync.SegmentMaintenancePolicy;
import org.hestiastore.index.segmentasync.SegmentMaintenanceQueue;
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
            assertTrue(adapter instanceof SegmentMaintenanceBlocking);
        } finally {
            executor.shutdownNow();
        }
    }
}
