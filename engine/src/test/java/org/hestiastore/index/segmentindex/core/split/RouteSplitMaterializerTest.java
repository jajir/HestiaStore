package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.NoSuchElementException;
import java.util.List;

import org.hestiastore.index.BusyRetryPolicy;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.OperationResult;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.routemap.RouteSplitPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RouteSplitMaterializerTest {

    @Mock
    private Segment<Integer, String> parentSegment;

    @Mock
    private PreparedSegmentMaterializer<Integer, String> materializationService;

    private RouteSplitMaterializer<Integer, String> preparationService;

    @BeforeEach
    void setUp() {
        preparationService = new RouteSplitMaterializer<>(
                materializationService, new BusyRetryPolicy(1, 1));
    }

    @Test
    void prepareReturnsMaterializedSplitForEligibleBoundary() {
        final RouteSplitPlan<Integer> routeSplit = new RouteSplitPlan<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 4, null);
        final EntryIterator<Integer, String> iterator = iterator(entries(8));
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(OperationResult.ok(iterator));
        when(materializationService.materializeRouteSplit(eq(parentSegment),
                eq(4L), eq(3L), any()))
                .thenReturn(RouteSplitPreparation.prepared(routeSplit));

        final RouteSplitPreparation<Integer> prepared = preparationService
                .prepare(parentSegment, 8L);

        assertEquals(RouteSplitPreparationStatus.PREPARED, prepared.status());
        assertTrue(iterator.wasClosed());
        verify(parentSegment, times(1))
                .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
        verify(materializationService).materializeRouteSplit(eq(parentSegment),
                eq(4L), eq(3L), any());
    }

    @Test
    void prepareClampsLowerTargetToMinimumChildSize() {
        final RouteSplitPlan<Integer> routeSplit = new RouteSplitPlan<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 3, null);
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(entries(6)));
        when(materializationService.materializeRouteSplit(eq(parentSegment),
                eq(3L), eq(3L), any()))
                .thenReturn(RouteSplitPreparation.prepared(routeSplit));

        final RouteSplitPreparation<Integer> prepared = preparationService
                .prepare(parentSegment, 2L);

        assertEquals(RouteSplitPreparationStatus.PREPARED, prepared.status());
        verify(materializationService).materializeRouteSplit(eq(parentSegment),
                eq(3L), eq(3L), any());
    }

    @Test
    void prepareReturnsSkippedWhenParentIsClosed() {
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(OperationResult.closed());

        final RouteSplitPreparation<Integer> prepared = preparationService
                .prepare(parentSegment, 10L);

        assertEquals(RouteSplitPreparationStatus.SKIPPED, prepared.status());
        verifyNoInteractions(materializationService);
    }

    @Test
    void prepareReturnsSkippedWhenIteratorInvalidates() {
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(entries(6)));
        when(materializationService.materializeRouteSplit(eq(parentSegment),
                eq(3L), eq(3L), any()))
                .thenThrow(new NoSuchElementException("invalidated"));

        final RouteSplitPreparation<Integer> prepared = preparationService
                .prepare(parentSegment, 6L);

        assertEquals(RouteSplitPreparationStatus.SKIPPED, prepared.status());
    }

    @Test
    void preparePropagatesCompactParentResult() {
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(entries(5)));
        when(materializationService.materializeRouteSplit(eq(parentSegment),
                eq(5L), eq(3L), any()))
                .thenReturn(RouteSplitPreparation.compactParent());

        final RouteSplitPreparation<Integer> prepared = preparationService
                .prepare(parentSegment, 10L);

        assertEquals(RouteSplitPreparationStatus.COMPACT_PARENT,
                prepared.status());
    }

    private static List<Entry<Integer, String>> entries(final int count) {
        return List.of(Entry.of(1, "a"), Entry.of(2, "b"), Entry.of(3, "c"),
                Entry.of(4, "d"), Entry.of(5, "e"), Entry.of(6, "f"),
                Entry.of(7, "g"), Entry.of(8, "h")).subList(0, count);
    }

    private static OperationResult<EntryIterator<Integer, String>> iteratorResult(
            final List<Entry<Integer, String>> entries) {
        return OperationResult.ok(iterator(entries));
    }

    private static EntryIterator<Integer, String> iterator(
            final List<Entry<Integer, String>> entries) {
        return EntryIterator.make(entries.iterator());
    }
}
