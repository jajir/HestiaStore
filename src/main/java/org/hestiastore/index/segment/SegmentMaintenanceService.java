package org.hestiastore.index.segment;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
     * @return result with completion stage
     */
    SegmentResult<CompletionStage<Void>> startMaintenance(
            final Supplier<SegmentMaintenanceWork> workSupplier) {
        return startMaintenance(workSupplier, null);
    }

    /**
     * Starts a maintenance operation and runs a callback after returning to
     * READY.
     *
     * @param workSupplier supplier of maintenance tasks
     * @param onReady optional callback to run after maintenance completes
     * @return result with completion stage
     */
    SegmentResult<CompletionStage<Void>> startMaintenance(
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
        final CompletableFuture<Void> completion = new CompletableFuture<>();
        try {
            maintenanceExecutor.execute(
                    () -> runMaintenance(work, completion, onReady));
        } catch (final RuntimeException e) {
            failUnlessClosed();
            return SegmentResult.error();
        }
        return SegmentResult.ok(completion);
    }

    /**
     * Executes maintenance work and transitions the gate through states.
     *
     * @param work maintenance work bundle
     * @param completion completion stage to resolve
     */
    private void runMaintenance(final SegmentMaintenanceWork work,
            final CompletableFuture<Void> completion, final Runnable onReady) {
        try {
            work.ioWork().run();
        } catch (final RuntimeException e) {
            failUnlessClosed();
            completion.completeExceptionally(e);
            return;
        }
        if (gate.getState() == SegmentState.CLOSED) {
            completion.complete(null);
            return;
        }
        if (!gate.finishMaintenanceToFreeze()) {
            failUnlessClosed();
            completion.completeExceptionally(new IllegalStateException(
                    "Segment maintenance failed to transition to FREEZE."));
            return;
        }
        try {
            work.publishWork().run();
        } catch (final RuntimeException e) {
            failUnlessClosed();
            completion.completeExceptionally(e);
            return;
        }
        if (!gate.finishFreezeToReady()) {
            failUnlessClosed();
            completion.completeExceptionally(new IllegalStateException(
                    "Segment maintenance failed to transition to READY."));
            return;
        }
        if (onReady != null) {
            try {
                onReady.run();
            } catch (final RuntimeException e) {
                completion.completeExceptionally(e);
                return;
            }
        }
        completion.complete(null);
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
