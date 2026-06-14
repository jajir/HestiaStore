package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.mapping.SegmentRouteSplit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RouteSplitPreparationServiceTest {

    @Mock
    private Segment<Integer, String> parentSegment;

    @Mock
    private DefaultSegmentMaterializationService<Integer, String> materializationService;

    private RouteSplitPreparationService<Integer, String> preparationService;

    @BeforeEach
    void setUp() {
        preparationService = new RouteSplitPreparationService<>(
                materializationService, new SplitRetryPolicy(1, 1));
    }

    @Test
    void prepareReturnsMaterializedSplitForEligibleBoundary() {
        final SegmentRouteSplit<Integer> routeSplit = new SegmentRouteSplit<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 2, null);
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(entries()))
                .thenReturn(iteratorResult(entries()));
        when(materializationService.materializeRouteSplit(eq(parentSegment),
                eq(2L), any())).thenReturn(routeSplit);

        final SegmentRouteSplit<Integer> prepared = preparationService.prepare(
                parentSegment, 2L);

        assertNotNull(prepared);
        verify(parentSegment, times(2))
                .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
        verify(materializationService).materializeRouteSplit(eq(parentSegment),
                eq(2L), any());
    }

    @Test
    void prepareReturnsNullWhenBoundaryIsNotEligible() {
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(List.of(Entry.of(1, "a"))));

        final SegmentRouteSplit<Integer> prepared = preparationService.prepare(
                parentSegment, 2L);

        assertNull(prepared);
    }

    @Test
    void prepareReturnsMaterializedSplitWhenRecountFallsBelowThresholdButChildMinimumIsSatisfied() {
        final SegmentRouteSplit<Integer> routeSplit = new SegmentRouteSplit<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 3, null);
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(entries(6)))
                .thenReturn(iteratorResult(entries(6)));
        when(materializationService.materializeRouteSplit(eq(parentSegment),
                eq(3L), any())).thenReturn(routeSplit);

        final SegmentRouteSplit<Integer> prepared = preparationService.prepare(
                parentSegment, 10L);

        assertNotNull(prepared);
        verify(parentSegment, times(2))
                .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
        verify(materializationService).materializeRouteSplit(eq(parentSegment),
                eq(3L), any());
    }

    @Test
    void prepareReturnsNullWhenRecountFallsBelowThresholdAndChildMinimumIsNotSatisfied() {
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(entries(5)));

        final SegmentRouteSplit<Integer> prepared = preparationService.prepare(
                parentSegment, 10L);

        assertNull(prepared);
        verifyNoInteractions(materializationService);
    }

    private static List<Entry<Integer, String>> entries() {
        return entries(4);
    }

    private static List<Entry<Integer, String>> entries(final int count) {
        return List.of(Entry.of(1, "a"), Entry.of(2, "b"), Entry.of(3, "c"),
                Entry.of(4, "d"), Entry.of(5, "e"), Entry.of(6, "f"))
                .subList(0, count);
    }

    private static OperationResult<EntryIterator<Integer, String>> iteratorResult(
            final List<Entry<Integer, String>> entries) {
        return OperationResult.ok(EntryIterator.make(entries.iterator()));
    }
}
