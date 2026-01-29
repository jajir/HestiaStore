package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.segmentregistry.SegmentRegistryGate;
import org.hestiastore.index.segmentregistry.SegmentRegistryState;
import org.junit.jupiter.api.Test;

class SegmentRegistryGateTest {

    @Test
    void starts_ready() {
        final SegmentRegistryGate gate = new SegmentRegistryGate();

        assertSame(SegmentRegistryState.READY, gate.getState());
    }

    @Test
    void tryEnterFreeze_moves_from_ready() {
        final SegmentRegistryGate gate = new SegmentRegistryGate();

        assertTrue(gate.tryEnterFreeze());
        assertSame(SegmentRegistryState.FREEZE, gate.getState());
    }

    @Test
    void tryEnterFreeze_fails_when_not_ready() {
        final SegmentRegistryGate gate = new SegmentRegistryGate();
        gate.tryEnterFreeze();

        assertFalse(gate.tryEnterFreeze());
        assertSame(SegmentRegistryState.FREEZE, gate.getState());
    }

    @Test
    void finishFreezeToReady_succeeds_from_freeze() {
        final SegmentRegistryGate gate = new SegmentRegistryGate();
        gate.tryEnterFreeze();

        assertTrue(gate.finishFreezeToReady());
        assertSame(SegmentRegistryState.READY, gate.getState());
    }

    @Test
    void close_sets_closed_from_ready() {
        final SegmentRegistryGate gate = new SegmentRegistryGate();

        assertTrue(gate.close());
        assertSame(SegmentRegistryState.CLOSED, gate.getState());
    }

    @Test
    void close_sets_closed_from_freeze() {
        final SegmentRegistryGate gate = new SegmentRegistryGate();
        gate.tryEnterFreeze();

        assertTrue(gate.close());
        assertSame(SegmentRegistryState.CLOSED, gate.getState());
    }

    @Test
    void close_is_idempotent() {
        final SegmentRegistryGate gate = new SegmentRegistryGate();

        assertTrue(gate.close());
        assertTrue(gate.close());
        assertSame(SegmentRegistryState.CLOSED, gate.getState());
    }

    @Test
    void fail_marks_error_and_prevents_close() {
        final SegmentRegistryGate gate = new SegmentRegistryGate();

        gate.fail();

        assertSame(SegmentRegistryState.ERROR, gate.getState());
        assertFalse(gate.close());
        assertSame(SegmentRegistryState.ERROR, gate.getState());
    }
}
