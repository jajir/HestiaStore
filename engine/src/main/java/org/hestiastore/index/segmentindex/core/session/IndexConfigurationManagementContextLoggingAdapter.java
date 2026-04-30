package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexConfigurationManagement;
import org.hestiastore.index.control.model.ConfigurationSnapshot;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimePatchValidation;

/**
 * MDC-aware wrapper around runtime configuration management.
 */
final class IndexConfigurationManagementContextLoggingAdapter
        implements IndexConfigurationManagement {

    private final IndexConfigurationManagement delegate;
    private final IndexMdcScopeRunner contextScopeRunner;

    IndexConfigurationManagementContextLoggingAdapter(
            final IndexConfigurationManagement delegate,
            final IndexMdcScopeRunner contextScopeRunner) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.contextScopeRunner = Vldtn.requireNonNull(contextScopeRunner,
                "contextScopeRunner");
    }

    @Override
    public ConfigurationSnapshot getConfigurationActual() {
        return contextScopeRunner.supply(delegate::getConfigurationActual);
    }

    @Override
    public ConfigurationSnapshot getConfigurationOriginal() {
        return contextScopeRunner.supply(delegate::getConfigurationOriginal);
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
