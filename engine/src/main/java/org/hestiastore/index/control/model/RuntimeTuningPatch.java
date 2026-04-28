package org.hestiastore.index.control.model;

import java.util.Map;

import org.hestiastore.index.Vldtn;

/**
 * Typed runtime-tuning patch request.
 */
public final class RuntimeTuningPatch {

    private final Map<RuntimeSettingKey, Integer> values;
    private final boolean dryRun;
    private final Long expectedRevision;

    RuntimeTuningPatch(final Map<RuntimeSettingKey, Integer> values,
            final boolean dryRun, final Long expectedRevision) {
        final Map<RuntimeSettingKey, Integer> input = Vldtn.requireNotEmptyMap(
                values, "values");
        for (final Map.Entry<RuntimeSettingKey, Integer> entry : input
                .entrySet()) {
            Vldtn.requireNonNull(entry.getKey(), "key");
            Vldtn.requireNonNull(entry.getValue(), "value");
        }
        if (expectedRevision != null) {
            Vldtn.requireGreaterThanOrEqualToZero(expectedRevision.longValue(),
                    "expectedRevision");
        }
        this.values = Map.copyOf(input);
        this.dryRun = dryRun;
        this.expectedRevision = expectedRevision;
    }

    /**
     * Creates a typed runtime-tuning patch builder.
     *
     * @return builder
     */
    public static RuntimeTuningPatchBuilder builder() {
        return new RuntimeTuningPatchBuilder();
    }

    /**
     * Converts this typed patch into the existing enum-map patch model.
     *
     * @return runtime config patch
     */
    public RuntimeConfigPatch toRuntimeConfigPatch() {
        return new RuntimeConfigPatch(values, dryRun, expectedRevision);
    }

    public Map<RuntimeSettingKey, Integer> values() {
        return values;
    }

    public Map<RuntimeSettingKey, Integer> getValues() {
        return values;
    }

    public boolean dryRun() {
        return dryRun;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public Long expectedRevision() {
        return expectedRevision;
    }

    public Long getExpectedRevision() {
        return expectedRevision;
    }
}
