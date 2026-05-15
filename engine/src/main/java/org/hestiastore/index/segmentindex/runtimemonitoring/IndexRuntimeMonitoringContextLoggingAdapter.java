package org.hestiastore.index.segmentindex.runtimemonitoring;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.logging.IndexMdcCallWrapper;

/**
 * MDC-aware wrapper around runtime monitoring snapshots.
 */
public final class IndexRuntimeMonitoringContextLoggingAdapter
        implements IndexRuntimeMonitoring {

    private final IndexRuntimeMonitoring delegate;
    private final IndexMdcCallWrapper contextCallWrapper;

    public IndexRuntimeMonitoringContextLoggingAdapter(
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
