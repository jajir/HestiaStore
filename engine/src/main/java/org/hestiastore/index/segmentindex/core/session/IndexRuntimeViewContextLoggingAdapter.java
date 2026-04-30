package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexRuntimeView;
import org.hestiastore.index.control.model.IndexRuntimeSnapshot;

/**
 * MDC-aware wrapper around runtime snapshot access.
 */
final class IndexRuntimeViewContextLoggingAdapter implements IndexRuntimeView {

    private final IndexRuntimeView delegate;
    private final IndexMdcScopeRunner contextScopeRunner;

    IndexRuntimeViewContextLoggingAdapter(final IndexRuntimeView delegate,
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
