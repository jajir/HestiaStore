package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

import java.util.List;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIteratorList;
import org.mockito.ArgumentMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentSplitStepReplaceIfNoRemainingTest {

    @Mock
    private SegmentDeltaCacheController<Integer, String> deltaCtrl;
    @Mock
    private SegmentPropertiesManager propsMgr;
    @Mock
    private SegmentPropertiesManager lowerProps;
    @Mock
    private SegmentImpl<Integer, String> lowerSeg;
    @Mock
    private SegmentFilesRenamer filesRenamer;
    @Mock
    private SegmentFiles<Integer, String> segmentFiles;
    @Mock
    private SegmentFiles<Integer, String> lowerFiles;

    private SegmentSplitStepReplaceIfNoRemaining<Integer, String> step;

    @BeforeEach
    void setup() {
        step = new SegmentSplitStepReplaceIfNoRemaining<>(new SegmentReplacer<>(
                filesRenamer, deltaCtrl, propsMgr, segmentFiles));
    }

    @AfterEach
    void tearDown() {
        step = null;
    }

    @Test
    void test_missing_ctx() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.perform(null, new SegmentSplitState<>()));
        assertEquals("Property 'ctx' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_state() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.perform(
                        new SegmentSplitContext<>(null, null, null, null),
                        null));
        assertEquals("Property 'state' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_iterator() {
        when(propsMgr.getSegmentStats()).thenReturn(new SegmentStats(0, 5, 0));
        when(deltaCtrl.getDeltaCacheSizeWithoutTombstones()).thenReturn(0);
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                null, null,
                SegmentSplitterPlan.fromPolicy(
                        new SegmentSplitterPolicy<>(propsMgr, deltaCtrl)),
                null);
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.perform(ctx, new SegmentSplitState<>()));
        assertEquals("Property 'iterator' must not be null.", err.getMessage());
    }

    @Test
    void test_missing_lowerSegment() {
        when(propsMgr.getSegmentStats()).thenReturn(new SegmentStats(0, 5, 0));
        when(deltaCtrl.getDeltaCacheSizeWithoutTombstones()).thenReturn(0);
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                null, null,
                SegmentSplitterPlan.fromPolicy(
                        new SegmentSplitterPolicy<>(propsMgr, deltaCtrl)),
                null);
        final SegmentSplitState<Integer, String> state = new SegmentSplitState<>();
        state.setIterator(
                new PairIteratorList<Integer, String>(java.util.List.of()));
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> step.perform(ctx, state));
        assertEquals("Property 'lowerSegment' must not be null.",
                err.getMessage());
    }

    @Test
    void replaces_when_no_remaining_and_returns_null_when_remaining() {
        // use class-level 'step', 'filesRenamer', and 'segmentFiles'

        // with items remaining
        final SegmentSplitState<Integer, String> state1 = new SegmentSplitState<>();
        state1.setIterator(new PairIteratorList<Integer, String>(List.of()));
        // Simulate remaining by custom iterator
        state1.setIterator(new PairIteratorList<Integer, String>(
                List.of(Pair.of(1, "a"))));
        // Plan with min/max set
        when(propsMgr.getSegmentStats()).thenReturn(new SegmentStats(0, 5, 0));
        when(deltaCtrl.getDeltaCacheSizeWithoutTombstones()).thenReturn(0);
        final SegmentSplitterPlan<Integer, String> plan = SegmentSplitterPlan
                .fromPolicy(new SegmentSplitterPolicy<>(propsMgr, deltaCtrl));
        plan.recordLower(org.hestiastore.index.Pair.of(0, "z"));
        final SegmentSplitContext<Integer, String> ctx = new SegmentSplitContext<>(
                null, null, plan, null);
        // Replace path: no elements remaining
        final SegmentSplitState<Integer, String> state2 = new SegmentSplitState<>();
        // use class-level lowerSeg and mock lowerFiles ad-hoc as needed
        // lowerFiles provided by class-level mock
        // lowerProps is class-level mock
        when(lowerProps.getSegmentStats())
                .thenReturn(new SegmentStats(0, 0, 0));
        when(lowerSeg.getSegmentFiles()).thenReturn(lowerFiles);
        when(lowerSeg.getSegmentPropertiesManager()).thenReturn(lowerProps);
        state2.setLowerSegment(lowerSeg);
        state2.setIterator(new PairIteratorList<Integer, String>(List.of()));
        final var result = step.perform(ctx, state2);
        assertNotNull(result);
        verify(filesRenamer, times(1)).renameFiles(any(), any());

        // Remaining path: iterator has next
        final SegmentSplitState<Integer, String> state3 = new SegmentSplitState<>();
        state3.setLowerSegment(lowerSeg);
        state3.setIterator(new PairIteratorList<Integer, String>(
                List.of(Pair.of(1, "a"))));
        final var result2 = step.perform(ctx, state3);
        assertNull(result2);
    }
}
