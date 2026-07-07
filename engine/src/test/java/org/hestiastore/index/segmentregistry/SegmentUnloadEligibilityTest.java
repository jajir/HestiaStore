package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SegmentUnloadEligibilityTest {

    @Test
    void canUnloadReturnsFalseForNullSegment() {
        final SegmentUnloadEligibility eligibility = new SegmentUnloadEligibility(
                new SegmentRegistryStateMachine());

        assertFalse(eligibility.canUnload(null));
    }

    @Test
    void canUnloadReturnsTrueForClosedSegment() {
        final SegmentUnloadEligibility eligibility = new SegmentUnloadEligibility(
                new SegmentRegistryStateMachine());
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        when(segment.getState()).thenReturn(SegmentState.CLOSED);

        assertTrue(eligibility.canUnload(segment));
    }

    @Test
    void canUnloadReturnsTrueForCleanReadySegmentWhenRegistryReady() {
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
        gate.finishFreezeToReady();
        final SegmentUnloadEligibility eligibility = new SegmentUnloadEligibility(
                gate);
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        when(segment.getId()).thenReturn(SegmentId.of(6));
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(segment.getNumberOfKeysInWriteCache()).thenReturn(0);

        assertTrue(eligibility.canUnload(segment));
    }

    @Test
    void canUnloadReturnsFalseForDirtyReadySegmentWhenRegistryReady() {
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
        gate.finishFreezeToReady();
        final SegmentUnloadEligibility eligibility = new SegmentUnloadEligibility(
                gate);
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        when(segment.getId()).thenReturn(SegmentId.of(7));
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(segment.getNumberOfKeysInWriteCache()).thenReturn(1);

        assertFalse(eligibility.canUnload(segment));
    }

    @Test
    void canForceUnloadReturnsTrueForDirtyReadySegmentWhenRegistryReady() {
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
        gate.finishFreezeToReady();
        final SegmentUnloadEligibility eligibility = new SegmentUnloadEligibility(
                gate);
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        when(segment.getId()).thenReturn(SegmentId.of(9));
        when(segment.getState()).thenReturn(SegmentState.READY);

        assertTrue(eligibility.canForceUnload(segment));
    }

    @Test
    void canForceUnloadReturnsFalseForReadySegmentWithoutId() {
        final SegmentUnloadEligibility eligibility = new SegmentUnloadEligibility(
                new SegmentRegistryStateMachine());
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        when(segment.getState()).thenReturn(SegmentState.READY);

        assertFalse(eligibility.canForceUnload(segment));
    }

    @Test
    void canUnloadReturnsTrueForDirtyReadySegmentWhenRegistryClosing() {
        final SegmentUnloadEligibility eligibility = new SegmentUnloadEligibility(
                new SegmentRegistryStateMachine());
        @SuppressWarnings("unchecked")
        final Segment<Integer, String> segment = Mockito.mock(Segment.class);
        when(segment.getId()).thenReturn(SegmentId.of(8));
        when(segment.getState()).thenReturn(SegmentState.READY);
        when(segment.getNumberOfKeysInWriteCache()).thenReturn(1);

        assertTrue(eligibility.canUnload(segment));
    }
}
