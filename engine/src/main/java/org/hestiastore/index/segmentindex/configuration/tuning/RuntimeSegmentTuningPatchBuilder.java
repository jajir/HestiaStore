package org.hestiastore.index.segmentindex.configuration.tuning;

import java.util.EnumMap;

import org.hestiastore.index.Vldtn;

/**
 * Builder section for runtime segment tuning values.
 */
public final class RuntimeSegmentTuningPatchBuilder {

    private final EnumMap<RuntimeSettingKey, RuntimeTuningValue> values;

    RuntimeSegmentTuningPatchBuilder(
            final EnumMap<RuntimeSettingKey, RuntimeTuningValue> values) {
        this.values = Vldtn.requireNonNull(values, "values");
    }

    public RuntimeSegmentTuningPatchBuilder cacheKeyLimit(final int value) {
        values.put(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                RuntimeTuningValue.ofInt(value));
        return this;
    }

    public RuntimeSegmentTuningPatchBuilder cachedSegmentLimit(
            final int value) {
        values.put(RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                RuntimeTuningValue.ofInt(value));
        return this;
    }
}
