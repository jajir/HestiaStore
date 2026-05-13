package org.hestiastore.index.segmentindex.configuration.tuning;

import java.util.EnumMap;

import org.hestiastore.index.Vldtn;

/**
 * Builder section for parsed chunk page cache runtime tuning values.
 */
public final class RuntimeChunkStoreCacheTuningPatchBuilder {

    private final EnumMap<RuntimeSettingKey, RuntimeTuningValue> values;

    RuntimeChunkStoreCacheTuningPatchBuilder(
            final EnumMap<RuntimeSettingKey, RuntimeTuningValue> values) {
        this.values = Vldtn.requireNonNull(values, "values");
    }

    public RuntimeChunkStoreCacheTuningPatchBuilder pageLimit(
            final int value) {
        values.put(RuntimeSettingKey.CHUNK_STORE_CACHE_PAGE_LIMIT,
                RuntimeTuningValue.ofInt(value));
        return this;
    }
}
