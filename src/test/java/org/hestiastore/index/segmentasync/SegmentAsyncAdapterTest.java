package org.hestiastore.index.segmentasync;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentAsyncAdapterTest {

    @Mock
    private Segment<String, String> delegate;

    @Test
    void putSchedulesFlushWhenPolicyRequestsIt() {
        when(delegate.put("key", "value")).thenReturn(SegmentResult.ok());
        when(delegate.wasClosed()).thenReturn(false);
        when(delegate.flush()).thenReturn(SegmentResult.ok());

        final SegmentMaintenancePolicy<String, String> policy = segment -> SegmentMaintenanceDecision
                .flushOnly();
        final SegmentAsyncAdapter<String, String> adapter = new SegmentAsyncAdapter<>(
                delegate, Runnable::run, policy);

        assertTrue(adapter.put("key", "value").isOk());

        verify(delegate).flush();
    }

    @Test
    void flushAsyncRunsOnScheduler() {
        when(delegate.wasClosed()).thenReturn(false);
        when(delegate.flush()).thenReturn(SegmentResult.ok());

        final SegmentAsyncAdapter<String, String> adapter = new SegmentAsyncAdapter<>(
                delegate, Runnable::run, SegmentMaintenancePolicy.none());

        final SegmentResult<Void> result = adapter.flushAsync()
                .toCompletableFuture().join();
        assertTrue(result.isOk());

        verify(delegate).flush();
    }
}
