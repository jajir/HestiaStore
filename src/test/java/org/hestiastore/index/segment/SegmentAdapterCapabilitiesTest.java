package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

class SegmentAdapterCapabilitiesTest {

    @Test
    void synchronizationAdapter_exposes_write_lock_support() {
        final Segment<Integer, String> segment = mock(Segment.class);
        final SegmentImplSynchronizationAdapter<Integer, String> adapter = new SegmentImplSynchronizationAdapter<>(
                segment);

        assertTrue(adapter instanceof SegmentWriteLockSupport);
    }

    // Async maintenance adapters are now owned by segmentindex.
}
