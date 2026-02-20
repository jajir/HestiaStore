package org.hestiastore.index.control.model;

import java.util.Map;

import org.hestiastore.index.Vldtn;

/**
 * Runtime-only patch request.
 */
public final class RuntimeConfigPatch {

    private final Map<RuntimeSettingKey, Integer> values;
    private final boolean dryRun;
    private final Long expectedRevision;

    /**
     * Creates validated patch.
     */
    public RuntimeConfigPatch(final Map<RuntimeSettingKey, Integer> values,
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

    public Map<RuntimeSettingKey, Integer> getValues() {
        return values;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public Long getExpectedRevision() {
        return expectedRevision;
    }

    // Backward-compatible accessor style for existing call sites.
    public Map<RuntimeSettingKey, Integer> values() {
        return values;
    }

    // Backward-compatible accessor style for existing call sites.
    public boolean dryRun() {
        return dryRun;
    }

    // Backward-compatible accessor style for existing call sites.
    public Long expectedRevision() {
        return expectedRevision;
    }
}
