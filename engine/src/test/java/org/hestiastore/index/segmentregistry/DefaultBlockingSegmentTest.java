package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    void checkAndRepairConsistencyRetriesBusyStatusUntilSuccessful() {
        when(segment.tryCheckAndRepairConsistency())
                .thenReturn(OperationResult.busy())
                .thenReturn(OperationResult.ok(17));

        assertEquals(17, handle.checkAndRepairConsistency());
        verify(segment, times(2)).tryCheckAndRepairConsistency();
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void checkAndRepairConsistencyReturnsNullForEmptySegment() {
        when(segment.tryCheckAndRepairConsistency())
                .thenReturn(OperationResult.ok(null));

        assertNull(handle.checkAndRepairConsistency());
        verify(segment).tryCheckAndRepairConsistency();
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void checkAndRepairConsistencyReloadsClosedSegment() {
        final Segment<Integer, String> reloadedSegment = mockSegment();
        when(segment.tryCheckAndRepairConsistency())
                .thenReturn(OperationResult.closed());
        when(segmentRegistry.loadSegment(SEGMENT_ID))
                .thenReturn(reloadedSegment);
        when(reloadedSegment.tryCheckAndRepairConsistency())
                .thenReturn(OperationResult.ok(19));

        assertEquals(19, handle.checkAndRepairConsistency());
        verify(segmentRegistry).loadSegment(SEGMENT_ID);
        verify(reloadedSegment).tryCheckAndRepairConsistency();
    }

    @Test
    void checkAndRepairConsistencyTimesOutWhenBusyPersists() {
        handle = newHandle(new BusyRetryPolicy(1, 5), false);
        when(segment.tryCheckAndRepairConsistency())
                .thenReturn(OperationResult.busy());

        final IndexException exception = assertThrows(IndexException.class,
                () -> handle.checkAndRepairConsistency());

        assertTrue(exception.getMessage().contains(
                "'checkAndRepairConsistency' timed out after 5 ms"));
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void checkAndRepairConsistencyPreservesInterrupt() {
        when(segment.tryCheckAndRepairConsistency())
                .thenReturn(OperationResult.busy());

        try {
            Thread.currentThread().interrupt();

            final IndexException exception = assertThrows(IndexException.class,
                    () -> handle.checkAndRepairConsistency());

            assertTrue(exception.getMessage().contains(
                    "'checkAndRepairConsistency' was interrupted"));
            assertTrue(Thread.currentThread().isInterrupted());
            verifyNoInteractions(segmentRegistry);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void checkAndRepairConsistencyDoesNotRetryErrorStatus() {
        when(segment.tryCheckAndRepairConsistency())
                .thenReturn(OperationResult.error());

        final IndexException exception = assertThrows(IndexException.class,
                () -> handle.checkAndRepairConsistency());

        assertEquals(
                "Segment 'segment-00007' failed to checkAndRepairConsistency: ERROR",
                exception.getMessage());
        verify(segment).tryCheckAndRepairConsistency();
        verifyNoInteractions(segmentRegistry);
    }

    @Test
    void checkAndRepairConsistencyPropagatesCorruption() {
        final IndexException corruption = new IndexException("broken keys");
        when(segment.tryCheckAndRepairConsistency()).thenThrow(corruption);

        final IndexException exception = assertThrows(IndexException.class,
                () -> handle.checkAndRepairConsistency());

        assertSame(corruption, exception);
        verify(segment).tryCheckAndRepairConsistency();
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
