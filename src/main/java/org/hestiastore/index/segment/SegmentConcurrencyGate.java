package org.hestiastore.index.segment;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Admission gate that coordinates state transitions with in-flight counters.
 */
final class SegmentConcurrencyGate {

    private static final int SPIN_LIMIT = 1_000;

    private final SegmentStateMachine stateMachine = new SegmentStateMachine();
    private final AtomicInteger inFlightReads = new AtomicInteger();
    private final AtomicInteger inFlightWrites = new AtomicInteger();
    private final AtomicBoolean closing = new AtomicBoolean(false);
    // TODO remove closeMonitor
    private final Object closeMonitor = new Object();

    /**
     * Returns the current segment state.
     *
     * @return current state
     */
    SegmentState getState() {
        return stateMachine.getState();
    }

    /**
     * Enters FREEZE for close and drains in-flight operations.
     *
     * @return true when close admission succeeded
     */
    boolean tryEnterCloseAndDrain() {
        if (!stateMachine.tryEnterFreeze()) {
            return false;
        }
        closing.set(true);
        signalCloseMonitor();
        return awaitNoInFlight();
    }

    /**
     * Attempts to enter a read operation.
     *
     * @return true when read admission succeeded
     */
    boolean tryEnterRead() {
        return tryEnterOperation(inFlightReads);
    }

    /**
     * Attempts to enter a write operation.
     *
     * @return true when write admission succeeded
     */
    boolean tryEnterWrite() {
        return tryEnterOperation(inFlightWrites);
    }

    /**
     * Marks completion of a read operation.
     */
    void exitRead() {
        inFlightReads.decrementAndGet();
        signalCloseMonitor();
    }

    /**
     * Marks completion of a write operation.
     */
    void exitWrite() {
        inFlightWrites.decrementAndGet();
        signalCloseMonitor();
    }

    /**
     * Transitions to FREEZE and waits for in-flight operations to drain.
     *
     * @return true when freeze and drain succeeded
     */
    boolean tryEnterFreezeAndDrain() {
        if (closing.get()) {
            return false;
        }
        if (!stateMachine.tryEnterFreeze()) {
            return false;
        }
        signalCloseMonitor();
        return awaitNoInFlight();
    }

    /**
     * Transitions from FREEZE to MAINTENANCE_RUNNING.
     *
     * @return true when transition succeeded
     */
    boolean enterMaintenanceRunning() {
        if (!stateMachine.enterMaintenanceRunning()) {
            return false;
        }
        signalCloseMonitor();
        return true;
    }

    /**
     * Transitions from MAINTENANCE_RUNNING to FREEZE and drains operations.
     *
     * @return true when transition succeeded
     */
    boolean finishMaintenanceToFreeze() {
        if (!stateMachine.finishMaintenanceToFreeze()) {
            return false;
        }
        signalCloseMonitor();
        return awaitNoInFlight();
    }

    /**
     * Transitions from FREEZE to READY.
     *
     * @return true when transition succeeded
     */
    boolean finishFreezeToReady() {
        if (!stateMachine.finishFreezeToReady()) {
            return false;
        }
        signalCloseMonitor();
        return true;
    }

    /**
     * Forces the segment into CLOSED state.
     */
    void forceClosed() {
        stateMachine.forceClosed();
        signalCloseMonitor();
    }

    /**
     * Transitions from FREEZE to CLOSED.
     *
     * @return true when transition succeeded
     */
    boolean finishCloseToClosed() {
        if (!stateMachine.finishFreezeToClosed()) {
            return false;
        }
        signalCloseMonitor();
        return true;
    }

    /**
     * Marks the segment as failed.
     */
    void fail() {
        stateMachine.fail();
        signalCloseMonitor();
    }

    /**
     * Returns the number of in-flight read operations.
     *
     * @return in-flight reads
     */
    int getInFlightReads() {
        return inFlightReads.get();
    }

    /**
     * Returns the number of in-flight write operations.
     *
     * @return in-flight writes
     */
    int getInFlightWrites() {
        return inFlightWrites.get();
    }

    /**
     * Attempts to enter a read or write operation.
     *
     * @param counter in-flight counter to increment
     * @return true when admission succeeded
     */
    private boolean tryEnterOperation(final AtomicInteger counter) {
        if (closing.get()) {
            return false;
        }
        SegmentState state = stateMachine.getState();
        if (!isOperationAllowed(state)) {
            return false;
        }
        counter.incrementAndGet();
        state = stateMachine.getState();
        if (!closing.get() && isOperationAllowed(state)) {
            return true;
        }
        counter.decrementAndGet();
        return false;
    }

    /**
     * Waits until there are no in-flight operations while in FREEZE.
     *
     * @return true when drained successfully
     */
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

    private void signalCloseMonitor() {
        synchronized (closeMonitor) {
            closeMonitor.notifyAll();
        }
    }

    /**
     * Returns true when there are reads or writes in flight.
     *
     * @return true when operations are in flight
     */
    private boolean hasInFlight() {
        return inFlightReads.get() > 0 || inFlightWrites.get() > 0;
    }

    /**
     * Returns whether operations are allowed in the given state.
     *
     * @param state segment state
     * @return true when operations may proceed
     */
    private static boolean isOperationAllowed(final SegmentState state) {
        return state == SegmentState.READY
                || state == SegmentState.MAINTENANCE_RUNNING;
    }
}
