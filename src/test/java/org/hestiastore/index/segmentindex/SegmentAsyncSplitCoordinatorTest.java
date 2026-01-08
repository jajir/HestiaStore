package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentAsyncSplitCoordinatorTest {

    @Test
    void optionallySplitAsync_runsSynchronouslyWithoutQueue() throws Exception {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = (Segment<Integer, String>) mock(
                Segment.class);
        @SuppressWarnings("unchecked")
        final SegmentSplitCoordinator<Integer, String> splitCoordinator = mock(
                SegmentSplitCoordinator.class);
        when(splitCoordinator.optionallySplit(segment, 10L)).thenReturn(true);

        final SegmentAsyncSplitCoordinator<Integer, String> coordinator = new SegmentAsyncSplitCoordinator<>(
                splitCoordinator, null);
        final CompletionStage<Boolean> result = coordinator
                .optionallySplitAsync(segment, 10L);

        assertTrue(result.toCompletableFuture().get(1, TimeUnit.SECONDS));
        verify(splitCoordinator).optionallySplit(segment, 10L);
    }

    @Test
    void optionallySplitAsync_deduplicatesInFlightSubmissions()
            throws Exception {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = (Segment<Integer, String>) mock(
                Segment.class);
        when(segment.getId()).thenReturn(SegmentId.of(1));

        final AtomicReference<Runnable> scheduled = new AtomicReference<>();
        final AtomicInteger submissions = new AtomicInteger();
        final java.util.concurrent.Executor executor = command -> {
            submissions.incrementAndGet();
            scheduled.set(command);
        };
        @SuppressWarnings("unchecked")
        final SegmentSplitCoordinator<Integer, String> splitCoordinator = mock(
                SegmentSplitCoordinator.class);
        when(splitCoordinator.optionallySplit(segment, 10L)).thenReturn(true);

        final SegmentAsyncSplitCoordinator<Integer, String> coordinator = new SegmentAsyncSplitCoordinator<>(
                splitCoordinator, executor);

        final CompletionStage<Boolean> first = coordinator
                .optionallySplitAsync(segment, 10L);
        final CompletionStage<Boolean> second = coordinator
                .optionallySplitAsync(segment, 10L);

        assertSame(first, second);
        assertTrue(submissions.get() == 1);

        assertNotNull(scheduled.get());
        scheduled.get().run();

        assertTrue(first.toCompletableFuture().get(1, TimeUnit.SECONDS));
        verify(splitCoordinator, times(1)).optionallySplit(segment, 10L);
    }
}
