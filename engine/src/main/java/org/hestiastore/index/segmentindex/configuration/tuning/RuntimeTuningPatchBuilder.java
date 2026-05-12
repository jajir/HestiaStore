package org.hestiastore.index.segmentindex.configuration.tuning;

import java.util.EnumMap;
import java.util.function.Consumer;

import org.hestiastore.index.Vldtn;

/**
 * Fluent builder for {@link RuntimeTuningPatch}.
 */
public final class RuntimeTuningPatchBuilder {

    private final EnumMap<RuntimeSettingKey, RuntimeTuningValue> values =
            new EnumMap<>(RuntimeSettingKey.class);
    private Long expectedRevision;

    RuntimeTuningPatchBuilder() {
    }

    public RuntimeTuningPatchBuilder segment(
            final Consumer<RuntimeSegmentTuningPatchBuilder> customizer) {
        Vldtn.requireNonNull(customizer, "customizer")
                .accept(new RuntimeSegmentTuningPatchBuilder(values));
        return this;
    }

    public RuntimeTuningPatchBuilder writePath(
            final Consumer<RuntimeWritePathTuningPatchBuilder> customizer) {
        Vldtn.requireNonNull(customizer, "customizer")
                .accept(new RuntimeWritePathTuningPatchBuilder(values));
        return this;
    }

    public RuntimeTuningPatchBuilder chunkStoreCache(
            final Consumer<RuntimeChunkStoreCacheTuningPatchBuilder> customizer) {
        Vldtn.requireNonNull(customizer, "customizer").accept(
                new RuntimeChunkStoreCacheTuningPatchBuilder(values));
        return this;
    }

    public RuntimeTuningPatchBuilder expectedRevision(final long value) {
        this.expectedRevision = Long.valueOf(value);
        return this;
    }

    public RuntimeTuningPatchBuilder expectedRevision(final Long value) {
        this.expectedRevision = value;
        return this;
    }

    public RuntimeTuningPatch build() {
        return new RuntimeTuningPatch(values, expectedRevision);
    }
}
