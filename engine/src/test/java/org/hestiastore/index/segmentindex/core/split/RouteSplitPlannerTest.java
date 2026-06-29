package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.configuration.api.IndexMaintenanceConfiguration;
import org.hestiastore.index.segmentindex.routemap.RouteSplitPlan;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RouteSplitPlannerTest {

    private static final SegmentId PARENT_SEGMENT_ID = SegmentId.of(1);
    private static final SegmentId LOWER_SEGMENT_ID = SegmentId.of(2);
    private static final SegmentId UPPER_SEGMENT_ID = SegmentId.of(3);

    @Mock
    private IndexMaintenanceConfiguration maintenance;

    @Mock
    private Segment<Integer, String> parentSegment;

    @Mock
    private BlockingSegment<Integer, String> parentHandle;

    @Mock
    private PreparedSegmentMaterializer<Integer, String> materializationService;

    private RouteSplitPlanner<Integer, String> coordinator;
    private RouteSplitPlan<Integer> splitPlan;

    @BeforeEach
    void setUp() {
        when(maintenance.busyBackoffMillis()).thenReturn(1);
        when(maintenance.busyTimeoutMillis()).thenReturn(1);
        coordinator = new RouteSplitPlanner<>(
                new RouteSplitMaterializer<>(materializationService,
                        new BusyRetryPolicy(maintenance.busyBackoffMillis(),
                                maintenance.busyTimeoutMillis())));
        splitPlan = new RouteSplitPlan<>(PARENT_SEGMENT_ID, LOWER_SEGMENT_ID,
                UPPER_SEGMENT_ID, 2, null);
    }

    @Test
    void tryPrepareSplitReturnsMaterializedSplit() {
        when(parentHandle.getId()).thenReturn(PARENT_SEGMENT_ID);
        when(parentHandle.getSegment()).thenReturn(parentSegment);
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(entries(8)));
        when(materializationService.materializeRouteSplit(
                eq(parentSegment), eq(4L), eq(3L), any()))
                .thenReturn(RouteSplitPreparation.prepared(splitPlan));

        final RouteSplitPreparation<Integer> prepared = coordinator
                .tryPrepareSplit(parentHandle, 2L, 8L);

        assertEquals(RouteSplitPreparationStatus.PREPARED, prepared.status());
        final RouteSplitPlan<Integer> routeSplit = prepared.routeSplit()
                .orElseThrow();
        assertSame(PARENT_SEGMENT_ID, routeSplit.getReplacedSegmentId());
        assertSame(LOWER_SEGMENT_ID, routeSplit.getLowerSegmentId());
        assertSame(UPPER_SEGMENT_ID, routeSplit.getUpperSegmentId());
        verify(materializationService).materializeRouteSplit(
                eq(parentSegment), eq(4L), eq(3L), any());
        verify(parentHandle, never()).getRuntime();
    }

    @Test
    void tryPrepareSplitReturnsSkippedWhenSegmentIsTooSmall() {
        final RouteSplitPreparation<Integer> prepared = coordinator
                .tryPrepareSplit(parentHandle, 2L, 1L);

        assertEquals(RouteSplitPreparationStatus.SKIPPED, prepared.status());
        verifyNoInteractions(materializationService);
    }

    @Test
    void tryPrepareSplitReturnsSkippedWhenThresholdIsNonPositive() {
        final RouteSplitPreparation<Integer> prepared = coordinator
                .tryPrepareSplit(parentHandle, 0L, 4L);

        assertEquals(RouteSplitPreparationStatus.SKIPPED, prepared.status());
        verifyNoInteractions(materializationService);
    }

    @Test
    void tryPrepareSplitUsesPassedEstimateInsteadOfRuntimeRecount() {
        when(parentHandle.getId()).thenReturn(PARENT_SEGMENT_ID);
        when(parentHandle.getSegment()).thenReturn(parentSegment);
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(entries(6)));
        when(materializationService.materializeRouteSplit(
                eq(parentSegment), eq(3L), eq(3L), any()))
                .thenReturn(RouteSplitPreparation.prepared(splitPlan));

        final RouteSplitPreparation<Integer> prepared = coordinator
                .tryPrepareSplit(parentHandle, 4L, 6L);

        assertEquals(RouteSplitPreparationStatus.PREPARED, prepared.status());
        verify(parentHandle, never()).getRuntime();
        verify(materializationService).materializeRouteSplit(
                eq(parentSegment), eq(3L), eq(3L), any());
    }

    @Test
    void tryPrepareSplitPropagatesCompactParentResult() {
        when(parentHandle.getId()).thenReturn(PARENT_SEGMENT_ID);
        when(parentHandle.getSegment()).thenReturn(parentSegment);
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(entries(5)));
        when(materializationService.materializeRouteSplit(
                eq(parentSegment), eq(5L), eq(3L), any()))
                .thenReturn(RouteSplitPreparation.compactParent());

        final RouteSplitPreparation<Integer> prepared = coordinator
                .tryPrepareSplit(parentHandle, 10L, 10L);

        assertEquals(RouteSplitPreparationStatus.COMPACT_PARENT,
                prepared.status());
        assertTrue(prepared.routeSplit().isEmpty());
    }

    private static List<Entry<Integer, String>> entries(final int count) {
        return List.of(Entry.of(1, "a"), Entry.of(2, "b"), Entry.of(3, "c"),
                Entry.of(4, "d"), Entry.of(5, "e"), Entry.of(6, "f"),
                Entry.of(7, "g"), Entry.of(8, "h")).subList(0, count);
    }

    private static OperationResult<EntryIterator<Integer, String>> iteratorResult(
            final List<Entry<Integer, String>> entries) {
        return OperationResult.ok(EntryIterator.make(entries.iterator()));
    }
}
