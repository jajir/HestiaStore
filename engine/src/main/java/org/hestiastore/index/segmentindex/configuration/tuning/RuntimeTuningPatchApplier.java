package org.hestiastore.index.segmentindex.configuration.tuning;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Vldtn;

/**
 * Applies validated runtime patches and triggers side effects for changed
 * settings.
 */
final class RuntimeTuningPatchApplier<K, V> {

    private final RuntimeTuningPatchValidator validator;
    private final RuntimeTuningState runtimeTuningState;
    private final RuntimeSegmentLimitApplier<K, V> effectiveLimitsApplier;
    private final SplitPolicyScanRequester splitScanRequester;

    RuntimeTuningPatchApplier(final RuntimeTuningPatchValidator validator,
            final RuntimeTuningState runtimeTuningState,
            final RuntimeSegmentLimitApplier<K, V> effectiveLimitsApplier,
            final SplitPolicyScanRequester splitScanRequester) {
        this.validator = Vldtn.requireNonNull(validator, "validator");
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
        this.effectiveLimitsApplier = Vldtn.requireNonNull(
                effectiveLimitsApplier, "effectiveLimitsApplier");
        this.splitScanRequester = Vldtn.requireNonNull(splitScanRequester,
                "splitScanRequester");
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
        effectiveLimitsApplier.apply(effective);
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
                RuntimeTuningKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE);
        addChange(changes, before, after,
                RuntimeTuningKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE);
        addChange(changes, before, after,
                RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT);
        addChange(changes, before, after,
                RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE);
        addChange(changes, before, after,
                RuntimeTuningKey.INDEX_BUFFERED_WRITE_KEY_LIMIT);
        addChange(changes, before, after,
                RuntimeTuningKey.SEGMENT_SPLIT_KEY_THRESHOLD);
        addChange(changes, before, after,
                RuntimeTuningKey.CHUNK_STORE_CACHE_PAGE_LIMIT);
        return List.copyOf(changes);
    }

    private static void addChange(final List<RuntimeTuningChange> changes,
            final RuntimeTuningSnapshot before,
            final RuntimeTuningSnapshot after, final RuntimeTuningKey key) {
        final RuntimeTuningValue beforeValue = before.value(key);
        final RuntimeTuningValue afterValue = after.value(key);
        if (!beforeValue.equals(afterValue)) {
            changes.add(new RuntimeTuningChange(key, beforeValue,
                    afterValue));
        }
    }

    private void notifySplitThresholdChangeIfNeeded(
            final List<RuntimeTuningChange> changes) {
        if (changes.stream().anyMatch(
                change -> change.field() == RuntimeTuningKey.SEGMENT_SPLIT_KEY_THRESHOLD)) {
            splitScanRequester.requestFullSplitScan();
        }
    }
}
