package org.hestiastore.index.segmentindex.runtimemonitoring;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.IndexMdcScopeRunner;

/**
 * MDC-aware wrapper around runtime monitoring snapshots.
 */
public final class IndexRuntimeMonitoringContextLoggingAdapter
        implements IndexRuntimeMonitoring {

    private final IndexRuntimeMonitoring delegate;
    private final IndexMdcScopeRunner contextScopeRunner;

    public IndexRuntimeMonitoringContextLoggingAdapter(
            final IndexRuntimeMonitoring delegate,
            final IndexMdcScopeRunner contextScopeRunner) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.contextScopeRunner = Vldtn.requireNonNull(contextScopeRunner,
                "contextScopeRunner");
    }

    @Override
    public IndexRuntimeSnapshot snapshot() {
        return contextScopeRunner.supply(delegate::snapshot);
    }
}
