package org.hestiastore.index.segmentindex.tuning;

import java.util.EnumMap;

import org.hestiastore.index.Vldtn;

/**
 * Builder section for runtime write-path tuning values.
 */
public final class RuntimeWritePathTuningPatchBuilder {

    private final EnumMap<RuntimeSettingKey, Integer> values;

    RuntimeWritePathTuningPatchBuilder(
            final EnumMap<RuntimeSettingKey, Integer> values) {
        this.values = Vldtn.requireNonNull(values, "values");
    }

    public RuntimeWritePathTuningPatchBuilder segmentWriteCacheKeyLimit(
            final Integer value) {
        values.put(RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT,
                value);
        return this;
    }

    public RuntimeWritePathTuningPatchBuilder maintenanceWriteCacheKeyLimit(
            final Integer value) {
        values.put(RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                value);
        return this;
    }

    public RuntimeWritePathTuningPatchBuilder indexBufferedWriteKeyLimit(
            final Integer value) {
        values.put(RuntimeSettingKey.INDEX_BUFFERED_WRITE_KEY_LIMIT, value);
        return this;
    }

    public RuntimeWritePathTuningPatchBuilder segmentSplitKeyThreshold(
            final Integer value) {
        values.put(
                RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD,
                value);
        return this;
    }
}
