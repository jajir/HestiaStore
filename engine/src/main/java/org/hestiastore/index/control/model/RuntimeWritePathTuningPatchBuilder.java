package org.hestiastore.index.control.model;

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
        values.put(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION,
                value);
        return this;
    }

    public RuntimeWritePathTuningPatchBuilder maintenanceWriteCacheKeyLimit(
            final Integer value) {
        values.put(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                value);
        return this;
    }

    public RuntimeWritePathTuningPatchBuilder indexBufferedWriteKeyLimit(
            final Integer value) {
        values.put(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER, value);
        return this;
    }

    public RuntimeWritePathTuningPatchBuilder segmentSplitKeyThreshold(
            final Integer value) {
        values.put(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                value);
        return this;
    }
}
