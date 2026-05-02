package org.hestiastore.index.segmentindex.core.control;

import java.util.Map;
import java.util.function.Consumer;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.runtimeconfiguration.ConfigurationSnapshot;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfigPatch;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimePatchResult;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimePatchValidation;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeSettingKey;

/**
 * Applies validated runtime patches and triggers side effects for changed
 * settings.
 */
final class RuntimeConfigPatchApplier {

    private final RuntimeConfigPatchValidator validator;
    private final RuntimeTuningState runtimeTuningState;
    private final Consumer<Map<RuntimeSettingKey, Integer>> effectiveLimitsApplier;
    private final Runnable splitThresholdChangedListener;

    RuntimeConfigPatchApplier(final RuntimeConfigPatchValidator validator,
            final RuntimeTuningState runtimeTuningState,
            final Consumer<Map<RuntimeSettingKey, Integer>> effectiveLimitsApplier,
            final Runnable splitThresholdChangedListener) {
        this.validator = Vldtn.requireNonNull(validator, "validator");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.effectiveLimitsApplier = Vldtn.requireNonNull(
                effectiveLimitsApplier, "effectiveLimitsApplier");
        this.splitThresholdChangedListener = Vldtn.requireNonNull(
                splitThresholdChangedListener, "splitThresholdChangedListener");
    }

    RuntimePatchResult apply(final RuntimeConfigPatch patch) {
        final RuntimePatchValidation validation = validator.validate(patch);
        if (!validation.valid() || isDryRun(patch)) {
            return new RuntimePatchResult(false, validation,
                    runtimeTuningState.snapshotCurrent());
        }
        final Map<RuntimeSettingKey, Integer> effective = runtimeTuningState
                .previewEffective(validation.normalizedValues());
        effectiveLimitsApplier.accept(effective);
        final ConfigurationSnapshot snapshot = runtimeTuningState
                .apply(validation.normalizedValues());
        notifySplitThresholdChangeIfNeeded(validation.normalizedValues());
        return new RuntimePatchResult(true, validation, snapshot);
    }

    private boolean isDryRun(final RuntimeConfigPatch patch) {
        return patch != null && patch.dryRun();
    }

    private void notifySplitThresholdChangeIfNeeded(
            final Map<RuntimeSettingKey, Integer> normalizedValues) {
        if (normalizedValues.containsKey(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT)) {
            splitThresholdChangedListener.run();
        }
    }
}
