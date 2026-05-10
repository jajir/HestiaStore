package org.hestiastore.index.segmentindex.configuration.tuning;

import java.util.EnumMap;

import org.hestiastore.index.Vldtn;

/**
 * Builder section for runtime write-path tuning values.
 */
public final class RuntimeWritePathTuningPatchBuilder {

    private final EnumMap<RuntimeSettingKey, RuntimeTuningValue> values;

    RuntimeWritePathTuningPatchBuilder(
            final EnumMap<RuntimeSettingKey, RuntimeTuningValue> values) {
        this.values = Vldtn.requireNonNull(values, "values");
    }

    public RuntimeWritePathTuningPatchBuilder segmentWriteCacheKeyLimit(
            final int value) {
        values.put(RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT,
                RuntimeTuningValue.ofInt(value));
        return this;
    }

    public RuntimeWritePathTuningPatchBuilder segmentWriteCacheKeyLimitDuringMaintenance(
            final int value) {
        values.put(RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                RuntimeTuningValue.ofInt(value));
        return this;
    }

    public RuntimeWritePathTuningPatchBuilder indexBufferedWriteKeyLimit(
            final int value) {
        values.put(RuntimeSettingKey.INDEX_BUFFERED_WRITE_KEY_LIMIT,
                RuntimeTuningValue.ofInt(value));
        return this;
    }

    public RuntimeWritePathTuningPatchBuilder segmentSplitKeyThreshold(
            final int value) {
        values.put(
                RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD,
                RuntimeTuningValue.ofInt(value));
        return this;
    }
}
