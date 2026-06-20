package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
    private BlockingSegment.Runtime parentRuntime;

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
        when(parentHandle.getRuntime()).thenReturn(parentRuntime);
        when(parentHandle.getId()).thenReturn(PARENT_SEGMENT_ID);
        when(parentRuntime.getNumberOfKeysInCache()).thenReturn(4L);
        when(parentHandle.getSegment()).thenReturn(parentSegment);
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(entries()))
                .thenReturn(iteratorResult(entries()));
        when(materializationService.materializeRouteSplit(
                eq(parentSegment), eq(2L), any()))
                .thenReturn(splitPlan);

        final RouteSplitPlan<Integer> prepared = coordinator
                .tryPrepareSplit(parentHandle, 2L);

        assertNotNull(prepared);
        assertSame(PARENT_SEGMENT_ID, prepared.getReplacedSegmentId());
        assertSame(LOWER_SEGMENT_ID, prepared.getLowerSegmentId());
        assertSame(UPPER_SEGMENT_ID, prepared.getUpperSegmentId());
        verify(materializationService).materializeRouteSplit(
                eq(parentSegment), eq(2L), any());
    }

    @Test
    void tryPrepareSplitReturnsNullWhenSegmentIsTooSmall() {
        when(parentHandle.getRuntime()).thenReturn(parentRuntime);
        when(parentRuntime.getNumberOfKeysInCache()).thenReturn(1L);

        final RouteSplitPlan<Integer> prepared = coordinator
                .tryPrepareSplit(parentHandle, 2L);

        assertNull(prepared);
        verifyNoInteractions(materializationService);
    }

    @Test
    void tryPrepareSplitReturnsNullWhenThresholdIsNonPositive() {
        when(parentHandle.getRuntime()).thenReturn(parentRuntime);
        when(parentRuntime.getNumberOfKeysInCache()).thenReturn(4L);

        final RouteSplitPlan<Integer> prepared = coordinator
                .tryPrepareSplit(parentHandle, 0L);

        assertNull(prepared);
        verifyNoInteractions(materializationService);
    }

    @Test
    void tryPrepareSplitReturnsNullWhenVisibleRecountFallsBelowThreshold() {
        when(parentHandle.getRuntime()).thenReturn(parentRuntime);
        when(parentHandle.getId()).thenReturn(PARENT_SEGMENT_ID);
        when(parentRuntime.getNumberOfKeysInCache()).thenReturn(4L);
        when(parentHandle.getSegment()).thenReturn(parentSegment);
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(List.of(Entry.of(1, "a"),
                        Entry.of(2, "b"), Entry.of(3, "c"))));

        final RouteSplitPlan<Integer> prepared = coordinator
                .tryPrepareSplit(parentHandle, 4L);

        assertNull(prepared);
        verifyNoInteractions(materializationService);
    }

    @Test
    void tryPrepareSplitContinuesWhenRecountFallsBelowThresholdButChildMinimumIsSatisfied() {
        when(parentHandle.getRuntime()).thenReturn(parentRuntime);
        when(parentHandle.getId()).thenReturn(PARENT_SEGMENT_ID);
        when(parentRuntime.getNumberOfKeysInCache()).thenReturn(10L);
        when(parentHandle.getSegment()).thenReturn(parentSegment);
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(List.of(Entry.of(1, "a"),
                        Entry.of(2, "b"), Entry.of(3, "c"),
                        Entry.of(4, "d"), Entry.of(5, "e"),
                        Entry.of(6, "f"))))
                .thenReturn(iteratorResult(List.of(Entry.of(1, "a"),
                        Entry.of(2, "b"), Entry.of(3, "c"),
                        Entry.of(4, "d"), Entry.of(5, "e"),
                        Entry.of(6, "f"))));
        when(materializationService.materializeRouteSplit(
                eq(parentSegment), eq(3L), any()))
                .thenReturn(splitPlan);

        final RouteSplitPlan<Integer> prepared = coordinator
                .tryPrepareSplit(parentHandle, 10L);

        assertNotNull(prepared);
        verify(materializationService).materializeRouteSplit(
                eq(parentSegment), eq(3L), any());
    }

    private static List<Entry<Integer, String>> entries() {
        return List.of(Entry.of(1, "a"), Entry.of(2, "b"), Entry.of(3, "c"),
                Entry.of(4, "d"));
    }

    private static OperationResult<EntryIterator<Integer, String>> iteratorResult(
            final List<Entry<Integer, String>> entries) {
        return OperationResult.ok(EntryIterator.make(entries.iterator()));
    }
}
