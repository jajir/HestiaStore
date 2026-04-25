package org.hestiastore.index.segmentindex.core.split;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segment.SegmentResult;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RouteSplitCoordinatorTest {

    private static final SegmentId PARENT_SEGMENT_ID = SegmentId.of(1);
    private static final SegmentId LOWER_SEGMENT_ID = SegmentId.of(2);
    private static final SegmentId UPPER_SEGMENT_ID = SegmentId.of(3);

    @Mock
    private IndexConfiguration<Integer, String> conf;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private Segment<Integer, String> parentSegment;

    @Mock
    private BlockingSegment<Integer, String> parentHandle;

    @Mock
    private BlockingSegment<Integer, String> currentHandle;

    @Mock
    private BlockingSegment.Runtime parentRuntime;

    @Mock
    private DefaultSegmentMaterializationService<Integer, String> materializationService;

    private RouteSplitCoordinator<Integer, String> coordinator;
    private RouteSplitPlan<Integer> splitPlan;

    @BeforeEach
    void setUp() {
        when(conf.getIndexBusyBackoffMillis()).thenReturn(1);
        when(conf.getIndexBusyTimeoutMillis()).thenReturn(1);
        coordinator = new RouteSplitCoordinator<>(conf, Integer::compare,
                segmentRegistry, materializationService);
        splitPlan = new RouteSplitPlan<>(PARENT_SEGMENT_ID, LOWER_SEGMENT_ID,
                UPPER_SEGMENT_ID, 1, 2, RouteSplitPlan.SplitMode.SPLIT);
    }

    @Test
    void tryPrepareSplitReturnsMaterializedSplit() {
        when(parentHandle.getRuntime()).thenReturn(parentRuntime);
        when(parentHandle.getId()).thenReturn(PARENT_SEGMENT_ID);
        when(parentRuntime.getNumberOfKeysInCache()).thenReturn(4L);
        when(parentHandle.getSegment()).thenReturn(parentSegment);
        when(segmentRegistry.tryGetSegment(PARENT_SEGMENT_ID))
                .thenReturn(Optional.of(parentHandle));
        when(parentSegment.openIterator(SegmentIteratorIsolation.FULL_ISOLATION))
                .thenReturn(iteratorResult(entries()), iteratorResult(entries()));
        when(materializationService.materializeRouteSplit(
                eq(parentSegment), eq(2L), any()))
                .thenReturn(splitPlan);

        final RouteSplitPlan<Integer> prepared = coordinator
                .tryPrepareSplit(parentHandle, 2L);

        assertNotNull(prepared);
        assertSame(PARENT_SEGMENT_ID, prepared.getReplacedSegmentId());
        assertSame(LOWER_SEGMENT_ID, prepared.getLowerSegmentId());
        assertSame(UPPER_SEGMENT_ID, prepared.getUpperSegmentId().orElseThrow());
        verify(materializationService).materializeRouteSplit(
                eq(parentSegment), eq(2L), any());
    }

    @Test
    void tryPrepareSplitReturnsNullWhenLoadedSegmentChanged() {
        when(parentHandle.getRuntime()).thenReturn(parentRuntime);
        when(parentHandle.getId()).thenReturn(PARENT_SEGMENT_ID);
        when(parentRuntime.getNumberOfKeysInCache()).thenReturn(4L);
        when(segmentRegistry.tryGetSegment(PARENT_SEGMENT_ID))
                .thenReturn(Optional.of(currentHandle));

        final RouteSplitPlan<Integer> prepared = coordinator
                .tryPrepareSplit(parentHandle, 2L);

        assertNull(prepared);
        verifyNoInteractions(materializationService);
    }

    private static List<Entry<Integer, String>> entries() {
        return List.of(Entry.of(1, "a"), Entry.of(2, "b"), Entry.of(3, "c"),
                Entry.of(4, "d"));
    }

    private static SegmentResult<EntryIterator<Integer, String>> iteratorResult(
            final List<Entry<Integer, String>> entries) {
        return SegmentResult.ok(EntryIterator.make(entries.iterator()));
    }
}
