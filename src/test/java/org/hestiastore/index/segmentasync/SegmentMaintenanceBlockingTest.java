package org.hestiastore.index.segmentasync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.junit.jupiter.api.Test;

class SegmentMaintenanceBlockingTest {

    @Test
    void blockingFlushAndCompact_delegateToSegment() {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> delegate = (Segment<Integer, String>) mock(
                Segment.class);
        when(delegate.flush()).thenReturn(SegmentResult.ok());
        when(delegate.compact()).thenReturn(SegmentResult.ok());

        final SegmentMaintenanceBlocking blocking = new SegmentAsyncAdapter<>(
                delegate, Runnable::run, SegmentMaintenancePolicy.none());

        assertEquals(SegmentResultStatus.OK,
                blocking.flushBlocking().getStatus());
        assertEquals(SegmentResultStatus.OK,
                blocking.compactBlocking().getStatus());

        verify(delegate).flush();
        verify(delegate).compact();
    }
}
