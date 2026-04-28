package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.OperationResult;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.IndexRetryPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
class RouteSplitPreparationServiceTest {

    @Mock
    private Segment<Integer, String> parentSegment;

    @Mock
    private DefaultSegmentMaterializationService<Integer, String> materializationService;

    @Mock
    private Logger logger;

    private RouteSplitPreparationService<Integer, String> preparationService;

    @BeforeEach
    void setUp() {
        preparationService = new RouteSplitPreparationService<>(
                materializationService, new IndexRetryPolicy(1, 1), logger);
    }

    @Test
    void prepareReturnsMaterializedSplitForEligibleBoundary() {
        final RouteSplitPlan<Integer> splitPlan = new RouteSplitPlan<>(
                SegmentId.of(1), SegmentId.of(2), SegmentId.of(3), 2,
                RouteSplitPlan.SplitMode.SPLIT);
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(entries()))
                .thenReturn(iteratorResult(entries()));
        when(materializationService.materializeRouteSplit(eq(parentSegment),
                eq(2L), any())).thenReturn(splitPlan);

        final RouteSplitPlan<Integer> prepared = preparationService.prepare(
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

        final RouteSplitPlan<Integer> prepared = preparationService.prepare(
                parentSegment, 2L);

        assertNull(prepared);
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
