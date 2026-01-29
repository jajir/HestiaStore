package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistryImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentAsyncSplitCoordinatorTest {

    @Mock
    private Segment<Integer, String> segment;

    @Mock
    private SegmentSplitCoordinator<Integer, String> splitCoordinator;
    @Mock
    private SegmentRegistryImpl<Integer, String> segmentRegistry;

    @Test
    void constructor_rejectsMissingExecutor() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentAsyncSplitCoordinator<>(splitCoordinator,
                        segmentRegistry,
                        null));

        assertEquals("Property 'splitExecutor' must not be null.",
                ex.getMessage());
    }

    @Test
    void optionallySplitAsync_deduplicatesInFlightSubmissions()
            throws Exception {
        when(segment.getId()).thenReturn(SegmentId.of(1));

        final AtomicReference<Runnable> scheduled = new AtomicReference<>();
        final AtomicInteger submissions = new AtomicInteger();
        final Executor executor = command -> {
            submissions.incrementAndGet();
            scheduled.set(command);
        };
        when(splitCoordinator.optionallySplit(segment, 10L)).thenReturn(true);

        final SegmentAsyncSplitCoordinator<Integer, String> coordinator = new SegmentAsyncSplitCoordinator<>(
                splitCoordinator, segmentRegistry, executor);

        final SegmentAsyncSplitCoordinator.SplitHandle first = coordinator
                .optionallySplitAsync(segment, 10L);
        final SegmentAsyncSplitCoordinator.SplitHandle second = coordinator
                .optionallySplitAsync(segment, 10L);

        assertSame(first, second);
        assertTrue(submissions.get() == 1);

        assertNotNull(scheduled.get());
        scheduled.get().run();

        assertTrue(first.completion().toCompletableFuture()
                .get(1, TimeUnit.SECONDS));
        verify(splitCoordinator, times(1)).optionallySplit(segment, 10L);
        verify(segmentRegistry, times(1)).markSplitInFlight(SegmentId.of(1));
        verify(segmentRegistry, times(1)).clearSplitInFlight(SegmentId.of(1));
    }
}
