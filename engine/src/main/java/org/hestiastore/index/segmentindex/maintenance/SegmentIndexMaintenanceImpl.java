package org.hestiastore.index.segmentindex.maintenance;

import org.hestiastore.index.Vldtn;

/**
 * Default {@link SegmentIndexMaintenance} implementation backed by index
 * runtime maintenance commands.
 */
public final class SegmentIndexMaintenanceImpl
        implements SegmentIndexMaintenance {

    private final Runnable compactAction;
    private final Runnable compactAndWaitAction;
    private final Runnable flushAction;
    private final Runnable flushAndWaitAction;
    private final Runnable consistencyRepairAction;

    public SegmentIndexMaintenanceImpl(final Runnable compactAction,
            final Runnable compactAndWaitAction, final Runnable flushAction,
            final Runnable flushAndWaitAction,
            final Runnable consistencyRepairAction) {
        this.compactAction = Vldtn.requireNonNull(compactAction,
                "compactAction");
        this.compactAndWaitAction = Vldtn.requireNonNull(compactAndWaitAction,
                "compactAndWaitAction");
        this.flushAction = Vldtn.requireNonNull(flushAction, "flushAction");
        this.flushAndWaitAction = Vldtn.requireNonNull(flushAndWaitAction,
                "flushAndWaitAction");
        this.consistencyRepairAction = Vldtn.requireNonNull(
                consistencyRepairAction, "consistencyRepairAction");
    }

    @Override
    public void compact() {
        compactAction.run();
    }

    @Override
    public void compactAndWait() {
        compactAndWaitAction.run();
    }

    @Override
    public void flush() {
        flushAction.run();
    }

    @Override
    public void flushAndWait() {
        flushAndWaitAction.run();
    }

    @Override
    public void checkAndRepairConsistency() {
        consistencyRepairAction.run();
    }
}
