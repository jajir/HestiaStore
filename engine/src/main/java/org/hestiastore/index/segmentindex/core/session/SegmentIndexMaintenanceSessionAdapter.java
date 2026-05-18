package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.maintenance.SegmentIndexMaintenance;

/**
 * Applies session lifecycle and operation tracking around maintenance calls.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SegmentIndexMaintenanceSessionAdapter<K, V>
        implements SegmentIndexMaintenance {

    private final SegmentIndexMaintenance delegate;
    private final SegmentIndexSessionOwner<K, V> sessionOwner;
    private final SegmentIndexTrackedOperationRunner<K, V> trackedRunner;

    SegmentIndexMaintenanceSessionAdapter(
            final SegmentIndexMaintenance delegate,
            final SegmentIndexSessionOwner<K, V> sessionOwner,
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.sessionOwner = Vldtn.requireNonNull(sessionOwner,
                "sessionOwner");
        this.trackedRunner = Vldtn.requireNonNull(trackedRunner,
                "trackedRunner");
    }

    @Override
    public void compact() {
        run(delegate::compact);
    }

    @Override
    public void compactAndWait() {
        run(delegate::compactAndWait);
    }

    @Override
    public void flush() {
        run(delegate::flush);
    }

    @Override
    public void flushAndWait() {
        run(delegate::flushAndWait);
    }

    @Override
    public void checkAndRepairConsistency() {
        run(delegate::checkAndRepairConsistency);
    }

    private void run(final Runnable action) {
        trackedRunner.runTrackedVoid(action);
        sessionOwner.invalidateSegmentIterators();
    }
}
