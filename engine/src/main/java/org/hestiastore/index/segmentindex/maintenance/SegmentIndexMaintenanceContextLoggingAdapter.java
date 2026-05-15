package org.hestiastore.index.segmentindex.maintenance;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.logging.IndexMdcCallWrapper;

/**
 * Adds index MDC context around maintenance commands.
 */
public final class SegmentIndexMaintenanceContextLoggingAdapter
        implements SegmentIndexMaintenance {

    private final SegmentIndexMaintenance delegate;
    private final IndexMdcCallWrapper contextCallWrapper;

    public SegmentIndexMaintenanceContextLoggingAdapter(
            final SegmentIndexMaintenance delegate,
            final IndexMdcCallWrapper contextCallWrapper) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.contextCallWrapper = Vldtn.requireNonNull(contextCallWrapper,
                "contextCallWrapper");
    }

    @Override
    public void compact() {
        contextCallWrapper.run(delegate::compact);
    }

    @Override
    public void compactAndWait() {
        contextCallWrapper.run(delegate::compactAndWait);
    }

    @Override
    public void flush() {
        contextCallWrapper.run(delegate::flush);
    }

    @Override
    public void flushAndWait() {
        contextCallWrapper.run(delegate::flushAndWait);
    }

    @Override
    public void checkAndRepairConsistency() {
        contextCallWrapper.run(delegate::checkAndRepairConsistency);
    }
}
