package org.hestiastore.index.segmentindex.maintenance;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.IndexMdcScopeRunner;

/**
 * Adds index MDC context around maintenance commands.
 */
public final class SegmentIndexMaintenanceContextLoggingAdapter
        implements SegmentIndexMaintenance {

    private final SegmentIndexMaintenance delegate;
    private final IndexMdcScopeRunner contextScopeRunner;

    public SegmentIndexMaintenanceContextLoggingAdapter(
            final SegmentIndexMaintenance delegate,
            final IndexMdcScopeRunner contextScopeRunner) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.contextScopeRunner = Vldtn.requireNonNull(contextScopeRunner,
                "contextScopeRunner");
    }

    @Override
    public void compact() {
        contextScopeRunner.run(delegate::compact);
    }

    @Override
    public void compactAndWait() {
        contextScopeRunner.run(delegate::compactAndWait);
    }

    @Override
    public void flush() {
        contextScopeRunner.run(delegate::flush);
    }

    @Override
    public void flushAndWait() {
        contextScopeRunner.run(delegate::flushAndWait);
    }

    @Override
    public void checkAndRepairConsistency() {
        contextScopeRunner.run(delegate::checkAndRepairConsistency);
    }
}
