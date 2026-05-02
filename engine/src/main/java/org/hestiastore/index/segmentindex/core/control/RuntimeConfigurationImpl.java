package org.hestiastore.index.segmentindex.core.control;

import java.util.Map;
import java.util.function.Consumer;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.runtimeconfiguration.ConfigurationSnapshot;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfiguration;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfigPatch;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimePatchResult;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimePatchValidation;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeSettingKey;

/**
 * Implements runtime patching over one index instance.
 */
public final class RuntimeConfigurationImpl
        implements RuntimeConfiguration {

    private final RuntimeConfigPatchValidator patchValidator;
    private final RuntimeConfigPatchApplier patchApplier;
    private final RuntimeTuningState runtimeTuningState;

    public RuntimeConfigurationImpl(
            final RuntimeTuningState runtimeTuningState,
            final Consumer<Map<RuntimeSettingKey, Integer>> effectiveLimitsApplier,
            final Runnable splitThresholdChangedListener) {
        final RuntimeTuningState validatedRuntimeTuningState = Vldtn
                .requireNonNull(runtimeTuningState, "runtimeTuningState");
        this.runtimeTuningState = validatedRuntimeTuningState;
        this.patchValidator = new RuntimeConfigPatchValidator(
                validatedRuntimeTuningState);
        this.patchApplier = new RuntimeConfigPatchApplier(this.patchValidator,
                validatedRuntimeTuningState,
                Vldtn.requireNonNull(effectiveLimitsApplier,
                        "effectiveLimitsApplier"),
                Vldtn.requireNonNull(splitThresholdChangedListener,
                        "splitThresholdChangedListener"));
    }

    @Override
    public ConfigurationSnapshot getCurrent() {
        return runtimeTuningState.snapshotCurrent();
    }

    @Override
    public ConfigurationSnapshot getOriginal() {
        return runtimeTuningState.snapshotOriginal();
    }

    @Override
    public RuntimePatchValidation validate(final RuntimeConfigPatch patch) {
        return patchValidator.validate(patch);
    }

    @Override
    public RuntimePatchResult apply(final RuntimeConfigPatch patch) {
        return patchApplier.apply(patch);
    }

}
