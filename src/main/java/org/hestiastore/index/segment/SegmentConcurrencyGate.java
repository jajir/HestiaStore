package org.hestiastore.index.segment;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Admission gate that coordinates state transitions with in-flight counters.
 */
final class SegmentConcurrencyGate {

    private static final int SPIN_LIMIT = 1_000;

    private final SegmentStateMachine stateMachine = new SegmentStateMachine();
    private final AtomicInteger inFlightReads = new AtomicInteger();
    private final AtomicInteger inFlightWrites = new AtomicInteger();

    SegmentState getState() {
        return stateMachine.getState();
    }

    boolean tryEnterRead() {
        return tryEnterOperation(inFlightReads);
    }

    boolean tryEnterWrite() {
        return tryEnterOperation(inFlightWrites);
    }

    void exitRead() {
        inFlightReads.decrementAndGet();
    }

    void exitWrite() {
        inFlightWrites.decrementAndGet();
    }

    boolean tryEnterFreezeAndDrain() {
        if (!stateMachine.tryEnterFreeze()) {
            return false;
        }
        return awaitNoInFlight();
    }

    boolean enterMaintenanceRunning() {
        return stateMachine.enterMaintenanceRunning();
    }

    boolean finishMaintenanceToFreeze() {
        return stateMachine.finishMaintenanceToFreeze();
    }

    boolean finishFreezeToReady() {
        return stateMachine.finishFreezeToReady();
    }

    void forceClosed() {
        stateMachine.forceClosed();
    }

    void fail() {
        stateMachine.fail();
    }

    int getInFlightReads() {
        return inFlightReads.get();
    }

    int getInFlightWrites() {
        return inFlightWrites.get();
    }

    private boolean tryEnterOperation(final AtomicInteger counter) {
        SegmentState state = stateMachine.getState();
        if (!isOperationAllowed(state)) {
            return false;
        }
        counter.incrementAndGet();
        state = stateMachine.getState();
        if (isOperationAllowed(state)) {
            return true;
        }
        counter.decrementAndGet();
        return false;
    }

    private boolean awaitNoInFlight() {
        int spins = 0;
        while (hasInFlight()) {
            if (stateMachine.getState() != SegmentState.FREEZE) {
                return false;
            }
            if (spins++ < SPIN_LIMIT) {
                Thread.onSpinWait();
            } else {
                Thread.yield();
                spins = 0;
            }
        }
        return stateMachine.getState() == SegmentState.FREEZE;
    }

    private boolean hasInFlight() {
        return inFlightReads.get() > 0 || inFlightWrites.get() > 0;
    }

    private static boolean isOperationAllowed(final SegmentState state) {
        return state == SegmentState.READY
                || state == SegmentState.MAINTENANCE_RUNNING;
    }
}
