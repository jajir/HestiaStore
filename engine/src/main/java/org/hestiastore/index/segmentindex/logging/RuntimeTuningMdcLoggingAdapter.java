package org.hestiastore.index.segmentindex.logging;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuning;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningPatch;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningResult;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningSnapshot;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningValidation;

/**
 * MDC-aware wrapper around runtime tuning management.
 */
public final class RuntimeTuningMdcLoggingAdapter implements RuntimeTuning {

    private final RuntimeTuning delegate;
    private final IndexMdcCallWrapper contextCallWrapper;

    public RuntimeTuningMdcLoggingAdapter(
            final RuntimeTuning delegate,
            final IndexMdcCallWrapper contextCallWrapper) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.contextCallWrapper = Vldtn.requireNonNull(contextCallWrapper,
                "contextCallWrapper");
    }

    @Override
    public RuntimeTuningSnapshot current() {
        return contextCallWrapper.supply(delegate::current);
    }

    @Override
    public RuntimeTuningSnapshot original() {
        return contextCallWrapper.supply(delegate::original);
    }

    @Override
    public RuntimeTuningValidation validate(final RuntimeTuningPatch patch) {
        return contextCallWrapper.supply(() -> delegate.validate(patch));
    }

    @Override
    public RuntimeTuningResult apply(final RuntimeTuningPatch patch) {
        return contextCallWrapper.supply(() -> delegate.apply(patch));
    }

    @Override
    public RuntimeTuningSnapshot persistCurrent() {
        return contextCallWrapper.supply(delegate::persistCurrent);
    }
}
