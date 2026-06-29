package org.hestiastore.index.segmentindex.configuration.tuning;

import java.util.Map;

import org.hestiastore.index.Vldtn;

/**
 * Typed runtime-tuning patch request.
 */
public final class RuntimeTuningPatch {

    private final Map<RuntimeTuningKey, RuntimeTuningValue> values;
    private final Long expectedRevision;

    RuntimeTuningPatch(
            final Map<RuntimeTuningKey, RuntimeTuningValue> values,
            final Long expectedRevision) {
        final Map<RuntimeTuningKey, RuntimeTuningValue> input =
                Vldtn.requireNotEmptyMap(values, "values");
        for (final Map.Entry<RuntimeTuningKey, RuntimeTuningValue> entry : input
                .entrySet()) {
            Vldtn.requireNonNull(entry.getKey(), "key");
            Vldtn.requireNonNull(entry.getValue(), "value");
        }
        if (expectedRevision != null) {
            Vldtn.requireGreaterThanOrEqualToZero(expectedRevision.longValue(),
                    "expectedRevision");
        }
        this.values = Map.copyOf(input);
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

    Map<RuntimeTuningKey, RuntimeTuningValue> values() {
        return values;
    }

    public Long expectedRevision() {
        return expectedRevision;
    }
}
