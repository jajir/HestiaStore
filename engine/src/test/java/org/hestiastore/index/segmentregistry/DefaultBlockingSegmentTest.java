package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentRuntimeLimits;
import org.hestiastore.index.segment.SegmentState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultBlockingSegmentTest {

    private static final SegmentId SEGMENT_ID = SegmentId.of(7);

    @Mock
    private BlockingSegmentRegistryAdapter<Integer, String> segmentRegistry;

    @Mock
    private Segment<Integer, String> segment;

    @Mock
    private SegmentRuntimeLimits runtimeLimits;

    private BlockingSegment<Integer, String> handle;

    @BeforeEach
    void setUp() {
        handle = newHandle(false);
    }

    @Test
    void getRetriesBusyStatusUntilSuccessful() {
        when(segment.get(11)).thenReturn(OperationResult.busy())
                .thenReturn(OperationResult.ok("value"));

        assertEquals("value", handle.get(11));
        verify(segment, times(2)).get(11);
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void putRetriesClosedStatusUntilSegmentReloads() {
        final Segment<Integer, String> reloadedSegment = mockSegment();
        when(segmentRegistry.loadSegment(SEGMENT_ID))
                .thenReturn(reloadedSegment);
        when(segment.put(1, "one")).thenReturn(OperationResult.closed());
        when(reloadedSegment.put(1, "one")).thenReturn(OperationResult.ok());

        handle.put(1, "one");

        verify(segment).put(1, "one");
        verify(reloadedSegment).put(1, "one");
        verify(segmentRegistry).loadSegment(SEGMENT_ID);
    }

    @Test
    void putRetriesWriteCacheFullStatusWhenMaintenanceEnabled() {
        handle = newHandle(true);
        when(segment.put(1, "one"))
                .thenReturn(OperationResult.writeCacheFull())
                .thenReturn(OperationResult.ok());

        handle.put(1, "one");

        verify(segment, times(2)).put(1, "one");
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void putThrowsImmediatelyWhenWriteCacheFullStatusReturnedAndMaintenanceDisabled() {
        when(segment.put(1, "one")).thenReturn(OperationResult.writeCacheFull());

        final IndexException exception = assertThrows(IndexException.class,
                () -> handle.put(1, "one"));

        assertEquals(
                "Write cache is full for segment 'segment-00007' and automatic maintenance is disabled.",
                exception.getMessage());
        verify(segment).put(1, "one");
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void putTimesOutWhenWriteCacheFullStatusPersistsAndMaintenanceEnabled() {
        handle = newHandle(new BusyRetryPolicy(1, 5), true);
        when(segment.put(1, "one"))
                .thenReturn(OperationResult.writeCacheFull());

        final IndexException exception = assertThrows(IndexException.class,
                () -> handle.put(1, "one"));

        assertTrue(exception.getMessage().contains("timed out"));
        assertFalse(exception.getMessage().contains(
                "automatic maintenance is disabled"));
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void compactThrowsOnErrorStatus() {
        when(segment.compact()).thenReturn(OperationResult.error());

        assertThrows(IndexException.class, () -> handle.compact());
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void updateRuntimeLimitsDelegatesToLoadedSegment() {
        when(segment.getState()).thenReturn(SegmentState.READY);

        handle.getRuntime().updateRuntimeLimits(runtimeLimits);

        verify(segment).applyRuntimeLimits(runtimeLimits);
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void tryGetReloadsClosedSegmentBeforeOperation() {
        final Segment<Integer, String> reloadedSegment = mockSegment();
        when(segment.getState()).thenReturn(SegmentState.CLOSED);
        when(segmentRegistry.loadSegment(SEGMENT_ID))
                .thenReturn(reloadedSegment);
        when(reloadedSegment.get(11)).thenReturn(OperationResult.ok("value"));

        final OperationResult<String> result = handle.tryGet(11);

        assertEquals("value", result.getValue());
        verify(segmentRegistry).loadSegment(SEGMENT_ID);
    }

    @Test
    void updateSegmentDoesNotReplaceLiveGeneration() {
        final Segment<Integer, String> staleSegment = mockSegment();
        when(segment.getState()).thenReturn(SegmentState.READY);

        ((DefaultBlockingSegment<Integer, String>) handle)
                .updateSegment(staleSegment);

        assertSame(segment, handle.getSegment());
        verifyNoInteractions(segmentRegistry);
    }

    private BlockingSegment<Integer, String> newHandle(
            final boolean automaticMaintenanceEnabled) {
        return newHandle(new BusyRetryPolicy(1, 25),
                automaticMaintenanceEnabled);
    }

    private BlockingSegment<Integer, String> newHandle(
            final BusyRetryPolicy retryPolicy,
            final boolean automaticMaintenanceEnabled) {
        return new DefaultBlockingSegment<>(SEGMENT_ID, segment,
                segmentRegistry,
                retryPolicy, automaticMaintenanceEnabled);
    }

    @SuppressWarnings("unchecked")
    private static <K, V> Segment<K, V> mockSegment() {
        return mock(Segment.class);
    }
}
