package org.hestiastore.index.segmentindex.runtimeconfiguration;

import java.util.Map;

import org.hestiastore.index.Vldtn;

/**
 * Runtime-only patch request.
 */
public record RuntimeConfigPatch(Map<RuntimeSettingKey, Integer> values,
        boolean dryRun, Long expectedRevision) {

    public RuntimeConfigPatch {
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
        values = Map.copyOf(input);
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
}
