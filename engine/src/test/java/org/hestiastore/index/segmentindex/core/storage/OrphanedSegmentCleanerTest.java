package org.hestiastore.index.segmentindex.core.storage;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

class OrphanedSegmentCleanerTest {

    @Test
    void remove_retriesUntilSegmentBecomesAvailable() {
        @SuppressWarnings("unchecked")
        final SegmentRegistry<Integer, String> segmentRegistry =
                mock(SegmentRegistry.class);
        final SegmentId segmentId = SegmentId.of(3);
        when(segmentRegistry.deleteSegmentIfAvailable(segmentId))
                .thenReturn(false, true);
        final OrphanedSegmentCleaner<Integer, String> remover =
                new OrphanedSegmentCleaner<>(segmentRegistry,
                        new BusyRetryPolicy(1, 100));

        remover.remove(segmentId);

        verify(segmentRegistry, times(2)).deleteSegmentIfAvailable(segmentId);
    }

    @Test
    void remove_swallowsIndexExceptionAndStopsRetrying() {
        @SuppressWarnings("unchecked")
        final SegmentRegistry<Integer, String> segmentRegistry =
                mock(SegmentRegistry.class);
        final SegmentId segmentId = SegmentId.of(3);
        when(segmentRegistry.deleteSegmentIfAvailable(segmentId))
                .thenThrow(new IndexException("boom"));
        final OrphanedSegmentCleaner<Integer, String> remover =
                new OrphanedSegmentCleaner<>(segmentRegistry,
                        new BusyRetryPolicy(2, 1));

        remover.remove(segmentId);

        verify(segmentRegistry).deleteSegmentIfAvailable(segmentId);
    }
}
