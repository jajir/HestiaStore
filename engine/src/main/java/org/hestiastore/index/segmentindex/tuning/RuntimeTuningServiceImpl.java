package org.hestiastore.index.segmentindex.tuning;

import java.util.Map;
import java.util.function.Consumer;

import org.hestiastore.index.Vldtn;

/**
 * Implements runtime patching over one index instance.
 */
public final class RuntimeTuningServiceImpl
        implements RuntimeConfiguration {

    private final RuntimeTuningPatchValidator patchValidator;
    private final RuntimeTuningPatchApplier patchApplier;
    private final RuntimeTuningState runtimeTuningState;

    public RuntimeTuningServiceImpl(final RuntimeTuningState runtimeTuningState,
            final Consumer<Map<RuntimeSettingKey, Integer>> effectiveLimitsApplier,
            final Runnable splitThresholdChangedListener) {
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.patchValidator = new RuntimeTuningPatchValidator(
                this.runtimeTuningState);
        this.patchApplier = new RuntimeTuningPatchApplier(patchValidator,
                this.runtimeTuningState,
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
