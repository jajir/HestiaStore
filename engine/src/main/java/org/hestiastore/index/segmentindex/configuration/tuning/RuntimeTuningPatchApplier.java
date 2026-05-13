package org.hestiastore.index.segmentindex.configuration.tuning;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.hestiastore.index.Vldtn;

/**
 * Applies validated runtime patches and triggers side effects for changed
 * settings.
 */
final class RuntimeTuningPatchApplier {

    private final RuntimeTuningPatchValidator validator;
    private final RuntimeTuningState runtimeTuningState;
    private final Consumer<RuntimeTuningSnapshot> effectiveLimitsApplier;
    private final Runnable splitThresholdChangedListener;

    RuntimeTuningPatchApplier(final RuntimeTuningPatchValidator validator,
            final RuntimeTuningState runtimeTuningState,
            final Consumer<RuntimeTuningSnapshot> effectiveLimitsApplier,
            final Runnable splitThresholdChangedListener) {
        this.validator = Vldtn.requireNonNull(validator, "validator");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.effectiveLimitsApplier = Vldtn.requireNonNull(
                effectiveLimitsApplier, "effectiveLimitsApplier");
        this.splitThresholdChangedListener = Vldtn.requireNonNull(
                splitThresholdChangedListener, "splitThresholdChangedListener");
    }

    RuntimeTuningResult apply(final RuntimeTuningPatch patch) {
        final RuntimeTuningSnapshot before =
                runtimeTuningState.snapshotCurrent();
        final RuntimeTuningValidation validation = validator.validate(patch);
        if (!validation.valid()) {
            return new RuntimeTuningResult(RuntimeTuningApplyStatus.REJECTED,
                    before, before, validation, List.of());
        }
        final RuntimeTuningSnapshot effective = runtimeTuningState
                .previewSnapshot(validation.normalizedValues());
        effectiveLimitsApplier.accept(effective);
        final RuntimeTuningSnapshot after = runtimeTuningState
                .apply(validation.normalizedValues());
        final List<RuntimeTuningChange> changes = changes(before, after);
        notifySplitThresholdChangeIfNeeded(changes);
        return new RuntimeTuningResult(RuntimeTuningApplyStatus.APPLIED,
                before, after, validation, changes);
    }

    private static List<RuntimeTuningChange> changes(
            final RuntimeTuningSnapshot before,
            final RuntimeTuningSnapshot after) {
        final List<RuntimeTuningChange> changes = new ArrayList<>();
        addChange(changes, before, after,
                RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE);
        addChange(changes, before, after,
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE);
        addChange(changes, before, after,
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT);
        addChange(changes, before, after,
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE);
        addChange(changes, before, after,
                RuntimeSettingKey.INDEX_BUFFERED_WRITE_KEY_LIMIT);
        addChange(changes, before, after,
                RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD);
        addChange(changes, before, after,
                RuntimeSettingKey.CHUNK_STORE_CACHE_PAGE_LIMIT);
        return List.copyOf(changes);
    }

    private static void addChange(final List<RuntimeTuningChange> changes,
            final RuntimeTuningSnapshot before,
            final RuntimeTuningSnapshot after, final RuntimeSettingKey key) {
        final RuntimeTuningValue beforeValue = before.value(key);
        final RuntimeTuningValue afterValue = after.value(key);
        if (!beforeValue.equals(afterValue)) {
            changes.add(new RuntimeTuningChange(key.field(), beforeValue,
                    afterValue));
        }
    }

    private void notifySplitThresholdChangeIfNeeded(
            final List<RuntimeTuningChange> changes) {
        if (changes.stream().anyMatch(
                change -> change.field() == RuntimeTuningField.WRITE_PATH_SEGMENT_SPLIT_KEY_THRESHOLD)) {
            splitThresholdChangedListener.run();
        }
    }
}
