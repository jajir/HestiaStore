package org.hestiastore.index.segmentindex.configuration.tuning;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;
import org.hestiastore.index.segmentindex.configuration.persistence.IndexConfigurationStore;

/**
 * Implements runtime patching over one index instance.
 */
public final class DefaultRuntimeTuning<K, V>
        implements RuntimeTuning {

    private final RuntimeTuningPatchValidator patchValidator;
    private final RuntimeTuningPatchApplier<K, V> patchApplier;
    private final RuntimeTuningState runtimeTuningState;
    private final EffectiveIndexConfiguration<K, V> configuration;
    private final IndexConfigurationStore<K, V> configurationStorage;

    public DefaultRuntimeTuning(final RuntimeTuningState runtimeTuningState,
            final RuntimeSegmentLimitApplier<K, V> effectiveLimitsApplier,
            final SplitPolicyScanRequester splitScanRequester,
            final EffectiveIndexConfiguration<K, V> configuration,
            final IndexConfigurationStore<K, V> configurationStorage) {
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.configuration = Vldtn.requireNonNull(configuration,
                "configuration");
        this.configurationStorage = Vldtn.requireNonNull(configurationStorage,
                "configurationStorage");
        this.patchValidator = new RuntimeTuningPatchValidator(
                this.runtimeTuningState);
        this.patchApplier = new RuntimeTuningPatchApplier<>(patchValidator,
                this.runtimeTuningState,
                Vldtn.requireNonNull(effectiveLimitsApplier,
                        "effectiveLimitsApplier"),
                Vldtn.requireNonNull(splitScanRequester,
                        "splitScanRequester"));
    }

    @Override
    public RuntimeTuningSnapshot current() {
        return runtimeTuningState.snapshotCurrent();
    }

    @Override
    public RuntimeTuningSnapshot original() {
        return runtimeTuningState.snapshotOriginal();
    }

    @Override
    public RuntimeTuningValidation validate(final RuntimeTuningPatch patch) {
        return patchValidator.validate(patch);
    }

    @Override
    public RuntimeTuningResult apply(final RuntimeTuningPatch patch) {
        return patchApplier.apply(patch);
    }

    @Override
    public RuntimeTuningSnapshot persistCurrent() {
        final RuntimeTuningSnapshot snapshot =
                runtimeTuningState.snapshotCurrent();
        configurationStorage.save(RuntimeTuningConfigurationMapper.apply(
                configuration, snapshot));
        return snapshot;
    }

}
