package org.hestiastore.index.segmentindex.core.control;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeConfigPatch;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimePatchValidation;
import org.hestiastore.index.segmentindex.runtimeconfiguration.RuntimeSettingKey;
import org.hestiastore.index.segmentindex.runtimeconfiguration.ValidationIssue;

/**
 * Validates runtime configuration patches and normalizes accepted values.
 */
final class RuntimeConfigPatchValidator {

    private static final int MIN_GENERAL_VALUE = 1;
    private static final int MIN_SEGMENT_CACHE_VALUE = 3;

    private final RuntimeTuningState runtimeTuningState;

    RuntimeConfigPatchValidator(final RuntimeTuningState runtimeTuningState) {
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
    }

    RuntimePatchValidation validate(final RuntimeConfigPatch patch) {
        final List<ValidationIssue> issues = new ArrayList<>();
        final EnumMap<RuntimeSettingKey, Integer> normalized = new EnumMap<>(
                RuntimeSettingKey.class);
        if (patch == null) {
            issues.add(new ValidationIssue(null, "patch must not be null"));
            return new RuntimePatchValidation(false, issues, normalized);
        }
        validateExpectedRevision(patch, issues);
        normalizePatchValues(patch, issues, normalized);
        validateEffectiveLimits(issues, normalized);
        return new RuntimePatchValidation(issues.isEmpty(), issues, normalized);
    }

    private void validateExpectedRevision(final RuntimeConfigPatch patch,
            final List<ValidationIssue> issues) {
        if (patch.expectedRevision() != null && patch.expectedRevision()
                .longValue() != runtimeTuningState.revision()) {
            issues.add(new ValidationIssue(null,
                    "expectedRevision does not match current revision"));
        }
    }

    private void normalizePatchValues(final RuntimeConfigPatch patch,
            final List<ValidationIssue> issues,
            final EnumMap<RuntimeSettingKey, Integer> normalized) {
        for (final Map.Entry<RuntimeSettingKey, Integer> entry : patch.values()
                .entrySet()) {
            final RuntimeSettingKey key = entry.getKey();
            final int value = entry.getValue().intValue();
            if (value < MIN_GENERAL_VALUE) {
                issues.add(new ValidationIssue(key, "value must be >= 1"));
                continue;
            }
            if (key == RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE
                    && value < MIN_SEGMENT_CACHE_VALUE) {
                issues.add(new ValidationIssue(key, "value must be >= 3"));
                continue;
            }
            normalized.put(key, Integer.valueOf(value));
        }
    }

    private void validateEffectiveLimits(final List<ValidationIssue> issues,
            final Map<RuntimeSettingKey, Integer> normalized) {
        final Map<RuntimeSettingKey, Integer> effective = runtimeTuningState
                .previewEffective(normalized);
        final int segmentWriteCacheLimit = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION)
                .intValue();
        final int maintenanceWriteCacheLimit = effective.get(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER)
                .intValue();
        final int indexBuffer = effective
                .get(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER)
                .intValue();
        if (maintenanceWriteCacheLimit <= segmentWriteCacheLimit) {
            issues.add(new ValidationIssue(
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                    "value must be greater than maxNumberOfKeysInActivePartition"));
        }
        if (indexBuffer < maintenanceWriteCacheLimit) {
            issues.add(new ValidationIssue(
                    RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER,
                    "value must be >= maxNumberOfKeysInPartitionBuffer"));
        }
    }
}
