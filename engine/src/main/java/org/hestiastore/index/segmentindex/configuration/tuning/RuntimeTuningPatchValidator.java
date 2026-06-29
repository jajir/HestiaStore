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
        final EnumMap<RuntimeTuningKey, RuntimeTuningValue> normalized =
                new EnumMap<>(RuntimeTuningKey.class);
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
            final EnumMap<RuntimeTuningKey, RuntimeTuningValue> normalized) {
        for (final Map.Entry<RuntimeTuningKey, RuntimeTuningValue> entry : patch
                .values().entrySet()) {
            final RuntimeTuningKey key = entry.getKey();
            final RuntimeTuningValue value = entry.getValue();
            final int intValue = value.asInt();
            if (key == RuntimeTuningKey.CHUNK_STORE_CACHE_PAGE_LIMIT) {
                normalizeChunkStoreCachePageLimit(issues, normalized, key,
                        value, intValue);
                continue;
            }
            if (intValue < MIN_GENERAL_VALUE) {
                issues.add(new RuntimeTuningValidationIssue(key,
                        "value must be >= 1"));
                continue;
            }
            if (key == RuntimeTuningKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE
                    && intValue < MIN_SEGMENT_CACHE_VALUE) {
                issues.add(new RuntimeTuningValidationIssue(key,
                        "value must be >= 3"));
                continue;
            }
            normalized.put(key, value);
        }
    }

    private void normalizeChunkStoreCachePageLimit(
            final List<RuntimeTuningValidationIssue> issues,
            final EnumMap<RuntimeTuningKey, RuntimeTuningValue> normalized,
            final RuntimeTuningKey key, final RuntimeTuningValue value,
            final int intValue) {
        if (intValue < 0) {
            issues.add(new RuntimeTuningValidationIssue(key,
                    "value must be >= 0"));
            return;
        }
        normalized.put(key, value);
    }

    private void validateEffectiveLimits(
            final List<RuntimeTuningValidationIssue> issues,
            final Map<RuntimeTuningKey, RuntimeTuningValue> normalized) {
        final Map<RuntimeTuningKey, RuntimeTuningValue> effective =
                runtimeTuningState.previewEffective(normalized);
        final int segmentWriteCacheLimit = effective.get(
                RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT)
                .asInt();
        final int maintenanceWriteCacheLimit = effective.get(
                RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE)
                .asInt();
        final int indexBuffer = effective
                .get(RuntimeTuningKey.INDEX_BUFFERED_WRITE_KEY_LIMIT)
                .asInt();
        if (maintenanceWriteCacheLimit <= segmentWriteCacheLimit) {
            issues.add(new RuntimeTuningValidationIssue(
                    RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                    "value must be greater than segmentWriteCacheKeyLimit"));
        }
        if (indexBuffer < maintenanceWriteCacheLimit) {
            issues.add(new RuntimeTuningValidationIssue(
                    RuntimeTuningKey.INDEX_BUFFERED_WRITE_KEY_LIMIT,
                    "value must be >= segmentWriteCacheKeyLimitDuringMaintenance"));
        }
    }
}
