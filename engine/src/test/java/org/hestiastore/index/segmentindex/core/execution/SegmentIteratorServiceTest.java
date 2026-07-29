package org.hestiastore.index.segmentindex.core.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.core.routing.MappedSegmentLeaseService;
import org.junit.jupiter.api.Test;

class SegmentIteratorServiceTest {

    @Test
    @SuppressWarnings("unchecked")
    void createBuildsSegmentStreamingService() {
        final MappedSegmentLeaseService<Integer, String> segmentLeaseService = mock(
                MappedSegmentLeaseService.class);

        final SegmentIteratorService<Integer, String> service =
                SegmentIteratorService.create(segmentLeaseService, 1, 10);

        assertNotNull(service);
    }

    @Test
    void createRejectsMissingSegmentLeaseService() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> SegmentIteratorService.create(null, 1, 10));

        assertEquals("Property 'segmentLeaseService' must not be null.",
                ex.getMessage());
    }

    @Test
    @SuppressWarnings("unchecked")
    void failFastRangeIteratorRequestsOnlyIntersectingSegments() {
        final MappedSegmentLeaseService<Integer, String> segmentLeaseService = mock(
                MappedSegmentLeaseService.class);
        when(segmentLeaseService.getSegmentIds(10, 20))
                .thenReturn(List.of());
        final SegmentIteratorService<Integer, String> service =
                SegmentIteratorService.create(segmentLeaseService, 1, 10);

        try (EntryIterator<Integer, String> iterator = service.openRangeIterator(
                10, 20, SegmentIteratorIsolation.FAIL_FAST)) {
            assertFalse(iterator.hasNext());
        }

        verify(segmentLeaseService).getSegmentIds(10, 20);
    }
}
