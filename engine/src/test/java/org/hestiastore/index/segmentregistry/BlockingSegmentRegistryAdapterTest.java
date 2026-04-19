package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BlockingSegmentRegistryAdapterTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(7);

    @Mock
    private SegmentRegistryStatusAccess<Integer, String> registry;

    @Mock
    private Segment<Integer, String> segment;

    @Test
    void getSegment_retriesBusyUntilSegmentLoads() {
        when(registry.tryLoadSegment(SEGMENT_ID)).thenReturn(
                SegmentRegistryResult.busy(),
                SegmentRegistryResult.ok(segment));
        final BlockingSegmentRegistryAdapter<Integer, String> adapter =
                new BlockingSegmentRegistryAdapter<>(registry,
                        new BusyRetryPolicy(1, 50));

        final Segment<Integer, String> loaded = adapter.loadSegment(SEGMENT_ID);

        assertSame(segment, loaded);
        verify(registry, times(2)).tryLoadSegment(SEGMENT_ID);
    }

    @Test
    void findSegment_returnsEmptyWhenRegistryReturnsClosed() {
        when(registry.tryLoadSegment(SEGMENT_ID))
                .thenReturn(SegmentRegistryResult.closed());
        final BlockingSegmentRegistryAdapter<Integer, String> adapter =
                new BlockingSegmentRegistryAdapter<>(registry,
                        new BusyRetryPolicy(1, 50));

        assertTrue(adapter.tryGetSegment(SEGMENT_ID).isEmpty());
        verify(registry).tryLoadSegment(SEGMENT_ID);
    }

    @Test
    void deleteSegment_retriesBusyUntilClosed() {
        when(registry.tryDeleteSegment(SEGMENT_ID)).thenReturn(
                SegmentRegistryResult.busy(), SegmentRegistryResult.closed());
        final BlockingSegmentRegistryAdapter<Integer, String> adapter =
                new BlockingSegmentRegistryAdapter<>(registry,
                        new BusyRetryPolicy(1, 50));

        adapter.deleteSegment(SEGMENT_ID);
        verify(registry, times(2)).tryDeleteSegment(SEGMENT_ID);
    }

    @Test
    void deleteSegmentIfAvailable_returnsFalseWhenRegistryStaysBusy() {
        when(registry.tryDeleteSegment(SEGMENT_ID)).thenReturn(
                SegmentRegistryResult.busy(), SegmentRegistryResult.busy());
        final BlockingSegmentRegistryAdapter<Integer, String> adapter =
                new BlockingSegmentRegistryAdapter<>(registry,
                        new BusyRetryPolicy(1, 50));

        assertFalse(adapter.deleteSegmentIfAvailable(SEGMENT_ID));
    }

    @Test
    void getSegment_throwsWhenRegistryReturnsError() {
        when(registry.tryLoadSegment(SEGMENT_ID))
                .thenReturn(SegmentRegistryResult.error());
        final BlockingSegmentRegistryAdapter<Integer, String> adapter =
                new BlockingSegmentRegistryAdapter<>(registry,
                        new BusyRetryPolicy(1, 50));

        assertThrows(IndexException.class,
                () -> adapter.loadSegment(SEGMENT_ID));
    }
}
