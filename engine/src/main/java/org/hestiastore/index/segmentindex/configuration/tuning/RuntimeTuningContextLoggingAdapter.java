package org.hestiastore.index.segmentindex.configuration.tuning;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.IndexMdcScopeRunner;

/**
 * MDC-aware wrapper around runtime tuning management.
 */
public final class RuntimeTuningContextLoggingAdapter implements RuntimeTuning {

    private final RuntimeTuning delegate;
    private final IndexMdcScopeRunner contextScopeRunner;

    public RuntimeTuningContextLoggingAdapter(
            final RuntimeTuning delegate,
            final IndexMdcScopeRunner contextScopeRunner) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.contextScopeRunner = Vldtn.requireNonNull(contextScopeRunner,
                "contextScopeRunner");
    }

    @Override
    public RuntimeTuningSnapshot current() {
        return contextScopeRunner.supply(delegate::current);
    }

    @Override
    public RuntimeTuningSnapshot original() {
        return contextScopeRunner.supply(delegate::original);
    }

    @Override
    public RuntimeTuningValidation validate(final RuntimeTuningPatch patch) {
        return contextScopeRunner.supply(() -> delegate.validate(patch));
    }

    @Override
    public RuntimeTuningResult apply(final RuntimeTuningPatch patch) {
        return contextScopeRunner.supply(() -> delegate.apply(patch));
    }

    @Override
    public RuntimeTuningSnapshot persistCurrent() {
        return contextScopeRunner.supply(delegate::persistCurrent);
    }
}
