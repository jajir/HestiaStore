package org.hestiastore.index.segmentindex.configuration.tuning;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.hestiastore.index.Vldtn;

/**
 * Validates runtime configuration patches and normalizes accepted values.
 */
final class RuntimeTuningPatchValidator {

    private static final int MIN_GENERAL_VALUE = 1;
    private static final int MIN_SEGMENT_CACHE_VALUE = 3;

    private final RuntimeTuningState runtimeTuningState;

    RuntimeTuningPatchValidator(final RuntimeTuningState runtimeTuningState) {
        this.runtimeTuningState = Vldtn.requireNonNull(runtimeTuningState,
                "runtimeTuningState");
    }

    RuntimeTuningValidation validate(final RuntimeTuningPatch patch) {
        final List<RuntimeTuningValidationIssue> issues = new ArrayList<>();
        final EnumMap<RuntimeSettingKey, RuntimeTuningValue> normalized =
                new EnumMap<>(RuntimeSettingKey.class);
        if (patch == null) {
            issues.add(new RuntimeTuningValidationIssue(null,
                    "patch must not be null"));
            return new RuntimeTuningValidation(false, issues, normalized);
        }
        validateExpectedRevision(patch, issues);
        normalizePatchValues(patch, issues, normalized);
        validateEffectiveLimits(issues, normalized);
        return new RuntimeTuningValidation(issues.isEmpty(), issues,
                normalized);
    }

    private void validateExpectedRevision(final RuntimeTuningPatch patch,
            final List<RuntimeTuningValidationIssue> issues) {
        if (patch.expectedRevision() != null && patch.expectedRevision()
                .longValue() != runtimeTuningState.revision()) {
            issues.add(new RuntimeTuningValidationIssue(null,
                    "expectedRevision does not match current revision"));
        }
    }

    private void normalizePatchValues(final RuntimeTuningPatch patch,
            final List<RuntimeTuningValidationIssue> issues,
            final EnumMap<RuntimeSettingKey, RuntimeTuningValue> normalized) {
        for (final Map.Entry<RuntimeSettingKey, RuntimeTuningValue> entry : patch
                .values().entrySet()) {
            final RuntimeSettingKey key = entry.getKey();
            final RuntimeTuningValue value = entry.getValue();
            if (value.type() != key.field().valueType()) {
                issues.add(new RuntimeTuningValidationIssue(key.field(),
                        "value type must be " + key.field().valueType()));
                continue;
            }
            if (key.field().valueType() != RuntimeTuningValueType.INT) {
                normalized.put(key, value);
                continue;
            }
            final int intValue = value.asInt();
            if (key == RuntimeSettingKey.CHUNK_STORE_CACHE_PAGE_LIMIT) {
                normalizeChunkStoreCachePageLimit(issues, normalized, key,
                        value, intValue);
                continue;
            }
            if (intValue < MIN_GENERAL_VALUE) {
                issues.add(new RuntimeTuningValidationIssue(key.field(),
                        "value must be >= 1"));
                continue;
            }
            if (key == RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE
                    && intValue < MIN_SEGMENT_CACHE_VALUE) {
                issues.add(new RuntimeTuningValidationIssue(key.field(),
                        "value must be >= 3"));
                continue;
            }
            normalized.put(key, value);
        }
    }

    private void normalizeChunkStoreCachePageLimit(
            final List<RuntimeTuningValidationIssue> issues,
            final EnumMap<RuntimeSettingKey, RuntimeTuningValue> normalized,
            final RuntimeSettingKey key, final RuntimeTuningValue value,
            final int intValue) {
        if (intValue < 0) {
            issues.add(new RuntimeTuningValidationIssue(key.field(),
                    "value must be >= 0"));
            return;
        }
        normalized.put(key, value);
    }

    private void validateEffectiveLimits(
            final List<RuntimeTuningValidationIssue> issues,
            final Map<RuntimeSettingKey, RuntimeTuningValue> normalized) {
        final Map<RuntimeSettingKey, RuntimeTuningValue> effective =
                runtimeTuningState.previewEffective(normalized);
        final int segmentWriteCacheLimit = effective.get(
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT)
                .asInt();
        final int maintenanceWriteCacheLimit = effective.get(
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE)
                .asInt();
        final int indexBuffer = effective
                .get(RuntimeSettingKey.INDEX_BUFFERED_WRITE_KEY_LIMIT)
                .asInt();
        if (maintenanceWriteCacheLimit <= segmentWriteCacheLimit) {
            issues.add(new RuntimeTuningValidationIssue(
                    RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE
                            .field(),
                    "value must be greater than segmentWriteCacheKeyLimit"));
        }
        if (indexBuffer < maintenanceWriteCacheLimit) {
            issues.add(new RuntimeTuningValidationIssue(
                    RuntimeSettingKey.INDEX_BUFFERED_WRITE_KEY_LIMIT.field(),
                    "value must be >= segmentWriteCacheKeyLimitDuringMaintenance"));
        }
    }
}
