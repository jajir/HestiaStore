package org.hestiastore.index.segmentindex.core.control;

import java.util.function.Function;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.IndexConfigurationManagement;
import org.hestiastore.index.control.model.ConfigurationSnapshot;
import org.hestiastore.index.control.model.RuntimeConfigPatch;
import org.hestiastore.index.control.model.RuntimePatchResult;
import org.hestiastore.index.control.model.RuntimePatchValidation;

/**
 * Exposes current/original runtime configuration snapshots and patch actions.
 */
final class IndexRuntimeConfigurationManagement
        implements IndexConfigurationManagement {

    private final RuntimeTuningState runtimeTuningState;
    private final Function<RuntimeConfigPatch, RuntimePatchValidation> validator;
    private final Function<RuntimeConfigPatch, RuntimePatchResult> applier;

    IndexRuntimeConfigurationManagement(
            final RuntimeTuningState runtimeTuningState,
            final Function<RuntimeConfigPatch, RuntimePatchValidation> validator,
            final Function<RuntimeConfigPatch, RuntimePatchResult> applier) {
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.validator = Vldtn.requireNonNull(validator, "validator");
        this.applier = Vldtn.requireNonNull(applier, "applier");
    }

    @Override
    public ConfigurationSnapshot getConfigurationActual() {
        return runtimeTuningState.snapshotCurrent();
    }

    @Override
    public ConfigurationSnapshot getConfigurationOriginal() {
        return runtimeTuningState.snapshotOriginal();
    }

    @Override
    public RuntimePatchValidation validate(final RuntimeConfigPatch patch) {
        return validator.apply(patch);
    }

    @Override
    public RuntimePatchResult apply(final RuntimeConfigPatch patch) {
        return applier.apply(patch);
    }
}
