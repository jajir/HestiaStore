package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SegmentRegistryStateMachineTest {

    @Test
    void startsReady() {
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();

        assertSame(SegmentRegistryState.READY, gate.getState());
    }

    @Test
    void tryEnterFreezeMovesFromReady() {
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();

        assertTrue(gate.tryEnterFreeze());
        assertSame(SegmentRegistryState.FREEZE, gate.getState());
    }

    @Test
    void tryEnterFreezeFailsWhenNotReady() {
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
        gate.tryEnterFreeze();

        assertFalse(gate.tryEnterFreeze());
        assertSame(SegmentRegistryState.FREEZE, gate.getState());
    }

    @Test
    void finishFreezeToReadySucceedsFromFreeze() {
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();
        gate.tryEnterFreeze();

        assertTrue(gate.finishFreezeToReady());
        assertSame(SegmentRegistryState.READY, gate.getState());
    }

    @Test
    void closeSetsClosedFromReady() {
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();

        assertTrue(gate.close());
        assertSame(SegmentRegistryState.CLOSED, gate.getState());
    }

    @Test
    void closeIsIdempotent() {
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();

        assertTrue(gate.close());
        assertTrue(gate.close());
        assertSame(SegmentRegistryState.CLOSED, gate.getState());
    }

    @Test
    void failMarksErrorAndPreventsClose() {
        final SegmentRegistryStateMachine gate = new SegmentRegistryStateMachine();

        gate.fail();

        assertSame(SegmentRegistryState.ERROR, gate.getState());
        assertFalse(gate.close());
        assertSame(SegmentRegistryState.ERROR, gate.getState());
    }
}
