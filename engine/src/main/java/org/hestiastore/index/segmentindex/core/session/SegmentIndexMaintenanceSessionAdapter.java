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
    private final SegmentTopologyRuntimeAccess<K, V> topologyRuntime;
    private final SegmentIndexTrackedOperationRunner<K, V> trackedRunner;

    /**
     * Creates a maintenance adapter that applies session operation tracking and
     * invalidates open iterators after successful maintenance commands.
     *
     * @param delegate maintenance command implementation
     * @param topologyRuntime topology runtime used to invalidate iterators
     * @param trackedRunner session operation tracker
     */
    SegmentIndexMaintenanceSessionAdapter(
            final SegmentIndexMaintenance delegate,
            final SegmentTopologyRuntimeAccess<K, V> topologyRuntime,
            final SegmentIndexTrackedOperationRunner<K, V> trackedRunner) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
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
        topologyRuntime.invalidateSegmentIterators();
    }
}
