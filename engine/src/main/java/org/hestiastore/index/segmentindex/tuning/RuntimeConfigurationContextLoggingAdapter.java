package org.hestiastore.index.segmentindex.tuning;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.core.IndexMdcScopeRunner;

/**
 * MDC-aware wrapper around runtime configuration management.
 */
public final class RuntimeConfigurationContextLoggingAdapter
        implements RuntimeConfiguration {

    private final RuntimeConfiguration delegate;
    private final IndexMdcScopeRunner contextScopeRunner;

    public RuntimeConfigurationContextLoggingAdapter(
            final RuntimeConfiguration delegate,
            final IndexMdcScopeRunner contextScopeRunner) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.contextScopeRunner = Vldtn.requireNonNull(contextScopeRunner,
                "contextScopeRunner");
    }

    @Override
    public ConfigurationSnapshot getCurrent() {
        return contextScopeRunner.supply(delegate::getCurrent);
    }

    @Override
    public ConfigurationSnapshot getOriginal() {
        return contextScopeRunner.supply(delegate::getOriginal);
    }

    @Override
    public RuntimePatchValidation validate(final RuntimeConfigPatch patch) {
        return contextScopeRunner.supply(() -> delegate.validate(patch));
    }

    @Override
    public RuntimePatchResult apply(final RuntimeConfigPatch patch) {
        return contextScopeRunner.supply(() -> delegate.apply(patch));
    }
}
