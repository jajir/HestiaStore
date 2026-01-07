package org.hestiastore.index.segmentasync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentResultStatus;
import org.junit.jupiter.api.Test;

class SegmentAsyncTest {

    @Test
    void asyncOperationsReturnCompletionStage() throws Exception {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> delegate = (Segment<Integer, String>) mock(
                Segment.class);
        when(delegate.flush()).thenReturn(SegmentResult.ok());
        when(delegate.compact()).thenReturn(SegmentResult.ok());

        final SegmentAsync<Integer, String> async = new SegmentAsyncAdapter<>(
                delegate, Runnable::run, SegmentMaintenancePolicy.none());

        assertEquals(SegmentResultStatus.OK,
                async.flushAsync().toCompletableFuture().get(1,
                        TimeUnit.SECONDS).getStatus());
        assertEquals(SegmentResultStatus.OK,
                async.compactAsync().toCompletableFuture().get(1,
                        TimeUnit.SECONDS).getStatus());
    }
}
