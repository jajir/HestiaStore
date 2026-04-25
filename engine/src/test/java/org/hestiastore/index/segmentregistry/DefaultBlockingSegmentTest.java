package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultBlockingSegmentTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(7);

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private Segment<Integer, String> segment;

    @Mock
    private SegmentRuntimeLimits runtimeLimits;

    private BlockingSegment<Integer, String> handle;

    @BeforeEach
    void setUp() {
        handle = new DefaultBlockingSegment<>(SEGMENT_ID,
                () -> segmentRegistry.loadSegment(SEGMENT_ID).getSegment(),
                new BusyRetryPolicy(1, 25));
    }

    @Test
    void getRetriesBusyStatusUntilSuccessful() {
        final BlockingSegment<Integer, String> segmentHandle = mock(
                BlockingSegment.class);
        when(segmentRegistry.loadSegment(SEGMENT_ID)).thenReturn(segmentHandle);
        when(segmentHandle.getSegment()).thenReturn(segment);
        when(segment.get(11)).thenReturn(SegmentResult.busy(),
                SegmentResult.ok("value"));

        assertEquals("value", handle.get(11));
        verify(segmentRegistry, times(2)).loadSegment(SEGMENT_ID);
        verify(segment, times(2)).get(11);
    }

    @Test
    void putRetriesClosedStatusUntilSegmentReloads() {
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> reloadedSegment = (Segment<Integer, String>) mock(
                Segment.class);
        final BlockingSegment<Integer, String> firstHandle = mock(
                BlockingSegment.class);
        final BlockingSegment<Integer, String> secondHandle = mock(
                BlockingSegment.class);
        when(segmentRegistry.loadSegment(SEGMENT_ID)).thenReturn(firstHandle,
                secondHandle);
        when(firstHandle.getSegment()).thenReturn(segment);
        when(secondHandle.getSegment()).thenReturn(reloadedSegment);
        when(segment.put(1, "one")).thenReturn(SegmentResult.closed());
        when(reloadedSegment.put(1, "one")).thenReturn(SegmentResult.ok());

        handle.put(1, "one");

        verify(segment).put(1, "one");
        verify(reloadedSegment).put(1, "one");
    }

    @Test
    void compactThrowsOnErrorStatus() {
        final BlockingSegment<Integer, String> segmentHandle = mock(
                BlockingSegment.class);
        when(segmentRegistry.loadSegment(SEGMENT_ID)).thenReturn(segmentHandle);
        when(segmentHandle.getSegment()).thenReturn(segment);
        when(segment.compact()).thenReturn(SegmentResult.error());

        assertThrows(IndexException.class, () -> handle.compact());
    }

    @Test
    void updateRuntimeLimitsDelegatesToLoadedSegment() {
        final BlockingSegment<Integer, String> segmentHandle = mock(
                BlockingSegment.class);
        when(segmentRegistry.loadSegment(SEGMENT_ID)).thenReturn(segmentHandle);
        when(segmentHandle.getSegment()).thenReturn(segment);

        handle.getRuntime().updateRuntimeLimits(runtimeLimits);

        verify(segment).applyRuntimeLimits(runtimeLimits);
    }
}
