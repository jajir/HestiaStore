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
    private final SegmentIndexTrackedOperationRunner trackedRunner;

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
            final SegmentIndexTrackedOperationRunner trackedRunner) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.topologyRuntime = Vldtn.requireNonNull(topologyRuntime,
                "topologyRuntime");
        this.trackedRunner = Vldtn.requireNonNull(trackedRunner,
                "trackedRunner");
    }

    @Override
    public void compact() {
        trackedRunner.runTrackedVoid(delegate::compact);
        topologyRuntime.invalidateSegmentIterators();
    }

    @Override
    public void compactAndWait() {
        trackedRunner.runTrackedVoid(delegate::compactAndWait);
        topologyRuntime.invalidateSegmentIterators();
    }

    @Override
    public void flush() {
        trackedRunner.runTrackedVoid(delegate::flush);
        topologyRuntime.invalidateSegmentIterators();
    }

    @Override
    public void flushAndWait() {
        trackedRunner.runTrackedVoid(delegate::flushAndWait);
        topologyRuntime.invalidateSegmentIterators();
    }

    @Override
    public void checkAndRepairConsistency() {
        trackedRunner.runTrackedVoid(delegate::checkAndRepairConsistency);
        topologyRuntime.requestFullSplitScan();
        topologyRuntime.invalidateSegmentIterators();
    }
}
