package org.hestiastore.index.segment;

import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;

/**
 * Coordinates segment maintenance execution and gate transitions.
 */
final class SegmentMaintenanceService {

    private final SegmentConcurrencyGate gate;
    private final Executor maintenanceExecutor;

    SegmentMaintenanceService(final SegmentConcurrencyGate gate,
            final Executor maintenanceExecutor) {
        this.gate = Vldtn.requireNonNull(gate, "gate");
        this.maintenanceExecutor = Vldtn.requireNonNull(maintenanceExecutor,
                "maintenanceExecutor");
    }

    /**
     * Starts a maintenance operation by freezing the segment and running work.
     *
     * @param workSupplier supplier of maintenance tasks
     * @return result status for the maintenance request
     */
    SegmentResult<Void> startMaintenance(
            final Supplier<SegmentMaintenanceWork> workSupplier) {
        return startMaintenance(workSupplier, null);
    }

    /**
     * Starts a maintenance operation and runs a callback after returning to
     * READY.
     *
     * @param workSupplier supplier of maintenance tasks
     * @param onReady optional callback to run after maintenance completes
     * @return result status for the maintenance request
     */
    SegmentResult<Void> startMaintenance(
            final Supplier<SegmentMaintenanceWork> workSupplier,
            final Runnable onReady) {
        Vldtn.requireNonNull(workSupplier, "workSupplier");
        if (!gate.tryEnterFreezeAndDrain()) {
            return resultForState(gate.getState());
        }
        final SegmentMaintenanceWork work;
        try {
            work = Vldtn.requireNonNull(workSupplier.get(), "work");
        } catch (final RuntimeException e) {
            failUnlessClosed();
            return SegmentResult.error();
        }
        if (!gate.enterMaintenanceRunning()) {
            failUnlessClosed();
            return SegmentResult.error();
        }
        try {
            maintenanceExecutor.execute(() -> runMaintenance(work, onReady));
        } catch (final RuntimeException e) {
            failUnlessClosed();
            return SegmentResult.error();
        }
        return SegmentResult.ok();
    }

    /**
     * Executes maintenance work and transitions the gate through states.
     *
     * @param work maintenance work bundle
     */
    private void runMaintenance(final SegmentMaintenanceWork work,
            final Runnable onReady) {
        try {
            work.ioWork().run();
        } catch (final RuntimeException e) {
            failUnlessClosed();
            return;
        }
        if (gate.getState() == SegmentState.CLOSED) {
            return;
        }
        if (!gate.finishMaintenanceToFreeze()) {
            failUnlessClosed();
            return;
        }
        try {
            work.publishWork().run();
        } catch (final RuntimeException e) {
            failUnlessClosed();
            return;
        }
        if (!gate.finishFreezeToReady()) {
            failUnlessClosed();
            return;
        }
        if (onReady != null) {
            try {
                onReady.run();
            } catch (final RuntimeException e) {
                gate.fail();
                return;
            }
        }
    }

    private static <T> SegmentResult<T> resultForState(
            final SegmentState state) {
        if (state == SegmentState.CLOSED) {
            return SegmentResult.closed();
        }
        if (state == SegmentState.ERROR) {
            return SegmentResult.error();
        }
        return SegmentResult.busy();
    }

    /**
     * Marks the segment ERROR unless it is already CLOSED.
     */
    private void failUnlessClosed() {
        if (gate.getState() != SegmentState.CLOSED) {
            gate.fail();
        }
    }
}
