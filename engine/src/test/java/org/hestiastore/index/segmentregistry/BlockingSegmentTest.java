package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.OperationStatus;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BlockingSegmentTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(7);

    @Mock
    private SegmentRegistryStatusAccess<Integer, String> registry;

    @Mock
    private Segment<Integer, String> segment;

    private BlockingSegment<Integer, String> handle;

    @BeforeEach
    void setUp() {
        final BusyRetryPolicy retryPolicy = new BusyRetryPolicy(1, 25);
        final BlockingSegmentRegistryAdapter<Integer, String> adapter =
                new BlockingSegmentRegistryAdapter<>(registry, retryPolicy);
        handle = new DefaultBlockingSegment<>(SEGMENT_ID, segment, adapter,
                retryPolicy, true);
    }

    @Test
    void lifecycleObservationDoesNotRetryBusyOrClosedRegistryStatuses() {
        when(segment.getState()).thenReturn(SegmentState.CLOSED);

        for (final OperationStatus status : List.of(OperationStatus.BUSY,
                OperationStatus.CLOSED)) {
            when(registry.tryLoadSegment(SEGMENT_ID))
                    .thenReturn(OperationResult.fromStatus(status));

            assertEquals(SegmentState.CLOSED, handle.getRuntime().getState());
        }

        verify(registry, times(2)).tryLoadSegment(SEGMENT_ID);
    }

    @Test
    void lifecycleObservationPropagatesRegistryErrorStatus() {
        when(segment.getState()).thenReturn(SegmentState.CLOSED);
        when(registry.tryLoadSegment(SEGMENT_ID))
                .thenReturn(OperationResult.error());

        final IndexException failure = assertThrows(IndexException.class,
                () -> handle.getRuntime().getState());

        assertEquals("Segment 'segment-00007' failed to load: ERROR",
                failure.getMessage());
        verify(registry).tryLoadSegment(SEGMENT_ID);
    }
}
