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
    private final Consumer<ConfigurationSnapshot> snapshotPersister;

    public RuntimeTuningServiceImpl(final RuntimeTuningState runtimeTuningState,
            final Consumer<Map<RuntimeSettingKey, Integer>> effectiveLimitsApplier,
            final Runnable splitThresholdChangedListener) {
        this(runtimeTuningState, effectiveLimitsApplier,
                splitThresholdChangedListener,
                RuntimeTuningServiceImpl::unsupportedPersistence);
    }

    public RuntimeTuningServiceImpl(final RuntimeTuningState runtimeTuningState,
            final Consumer<Map<RuntimeSettingKey, Integer>> effectiveLimitsApplier,
            final Runnable splitThresholdChangedListener,
            final Consumer<ConfigurationSnapshot> snapshotPersister) {
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.snapshotPersister = Vldtn.requireNonNull(snapshotPersister,
                "snapshotPersister");
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

    @Override
    public ConfigurationSnapshot persistCurrent() {
        final ConfigurationSnapshot snapshot =
                runtimeTuningState.snapshotCurrent();
        snapshotPersister.accept(snapshot);
        return snapshot;
    }

    private static void unsupportedPersistence(
            final ConfigurationSnapshot snapshot) {
        throw new UnsupportedOperationException(
                "Runtime tuning persistence is not available for this service.");
    }

}
