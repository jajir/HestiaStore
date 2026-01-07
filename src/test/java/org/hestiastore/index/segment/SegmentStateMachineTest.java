package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SegmentStateMachineTest {

    @Test
    void starts_in_ready_state() {
        final SegmentStateMachine machine = new SegmentStateMachine();

        assertEquals(SegmentState.READY, machine.getState());
    }

    @Test
    void maintenance_cycle_transitions_are_enforced() {
        final SegmentStateMachine machine = new SegmentStateMachine();

        assertTrue(machine.tryEnterFreeze());
        assertEquals(SegmentState.FREEZE, machine.getState());
        assertFalse(machine.tryEnterFreeze());

        assertTrue(machine.enterMaintenanceRunning());
        assertEquals(SegmentState.MAINTENANCE_RUNNING, machine.getState());
        assertFalse(machine.enterMaintenanceRunning());

        assertTrue(machine.finishMaintenanceToFreeze());
        assertEquals(SegmentState.FREEZE, machine.getState());

        assertTrue(machine.finishFreezeToReady());
        assertEquals(SegmentState.READY, machine.getState());
    }

    @Test
    void close_from_ready_sets_closed() {
        final SegmentStateMachine machine = new SegmentStateMachine();

        assertTrue(machine.closeFromReady());
        assertEquals(SegmentState.CLOSED, machine.getState());
        assertFalse(machine.closeFromReady());
    }

    @Test
    void fail_and_forceClosed_override_state() {
        final SegmentStateMachine machine = new SegmentStateMachine();

        machine.fail();
        assertEquals(SegmentState.ERROR, machine.getState());

        machine.forceClosed();
        assertEquals(SegmentState.CLOSED, machine.getState());
    }
}
