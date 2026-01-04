package org.hestiastore.index.segmentasync;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;

/**
 * Schedules maintenance tasks on a per-segment serial executor while applying a
 * maintenance policy.
 */
final class SegmentMaintenanceScheduler<K, V> {

    private final SerialExecutor maintenanceExecutor;
    private final SegmentMaintenancePolicy<K, V> maintenancePolicy;
    private final SegmentMaintenanceState maintenanceState = new SegmentMaintenanceState();

    SegmentMaintenanceScheduler(final Executor sharedExecutor,
            final SegmentMaintenancePolicy<K, V> maintenancePolicy) {
        this.maintenanceExecutor = new SerialExecutor(
                Vldtn.requireNonNull(sharedExecutor, "sharedExecutor"));
        this.maintenancePolicy = Vldtn.requireNonNull(maintenancePolicy,
                "maintenancePolicy");
    }

    CompletionStage<Void> submit(final SegmentMaintenanceTask taskType,
            final Runnable task) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            maintenanceExecutor.execute(() -> {
                maintenanceState.runExclusive(taskType, () -> {
                    try {
                        Vldtn.requireNonNull(task, "task").run();
                        future.complete(null);
                    } catch (final Throwable t) {
                        future.completeExceptionally(t);
                    }
                });
            });
        } catch (final RejectedExecutionException ex) {
            future.completeExceptionally(ex);
        }
        return future;
    }

    void scheduleAfterWrite(final Segment<K, V> segment,
            final Runnable flushTask, final Runnable compactTask) {
        final SegmentMaintenanceDecision decision = maintenancePolicy
                .evaluateAfterWrite(Vldtn.requireNonNull(segment, "segment"));
        if (decision.shouldFlush()) {
            submit(SegmentMaintenanceTask.FLUSH, flushTask);
        }
        if (decision.shouldCompact()) {
            submit(SegmentMaintenanceTask.COMPACT, compactTask);
        }
    }
}
