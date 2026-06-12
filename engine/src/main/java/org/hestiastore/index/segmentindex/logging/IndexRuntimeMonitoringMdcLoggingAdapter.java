package org.hestiastore.index.segmentindex.logging;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.runtimemonitoring.IndexRuntimeMonitoring;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.IndexRuntimeSnapshot;

/**
 * MDC-aware wrapper around runtime monitoring snapshots.
 */
public final class IndexRuntimeMonitoringMdcLoggingAdapter
        implements IndexRuntimeMonitoring {

    private final IndexRuntimeMonitoring delegate;
    private final IndexMdcCallWrapper contextCallWrapper;

    public IndexRuntimeMonitoringMdcLoggingAdapter(
            final IndexRuntimeMonitoring delegate,
            final IndexMdcCallWrapper contextCallWrapper) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.contextCallWrapper = Vldtn.requireNonNull(contextCallWrapper,
                "contextCallWrapper");
    }

    @Override
    public IndexRuntimeSnapshot snapshot() {
        return contextCallWrapper.supply(delegate::snapshot);
    }
}
