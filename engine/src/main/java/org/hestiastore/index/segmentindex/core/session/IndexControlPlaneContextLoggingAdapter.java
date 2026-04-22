package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexConfigurationManagement;
import org.hestiastore.index.control.IndexControlPlane;
import org.hestiastore.index.control.IndexRuntimeView;

/**
 * MDC-aware wrapper around index control-plane access.
 */
final class IndexControlPlaneContextLoggingAdapter
        implements IndexControlPlane {

    private final IndexControlPlane delegate;
    private final IndexContextScopeRunner contextScopeRunner;

    IndexControlPlaneContextLoggingAdapter(final IndexControlPlane delegate,
            final IndexContextScopeRunner contextScopeRunner) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.contextScopeRunner = Vldtn.requireNonNull(contextScopeRunner,
                "contextScopeRunner");
    }

    @Override
    public String indexName() {
        return contextScopeRunner.supply(delegate::indexName);
    }

    @Override
    public IndexRuntimeView runtime() {
        return contextScopeRunner.supply(() -> new IndexRuntimeViewContextLoggingAdapter(
                delegate.runtime(), contextScopeRunner));
    }

    @Override
    public IndexConfigurationManagement configuration() {
        return contextScopeRunner
                .supply(() -> new IndexConfigurationManagementContextLoggingAdapter(
                        delegate.configuration(), contextScopeRunner));
    }
}
