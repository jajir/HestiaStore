package org.hestiastore.index.segmentindex.runtimeconfiguration;

import java.util.EnumMap;
import java.util.function.Consumer;

import org.hestiastore.index.Vldtn;

/**
 * Fluent builder for {@link RuntimeTuningPatch}.
 */
public final class RuntimeTuningPatchBuilder {

    private final EnumMap<RuntimeSettingKey, Integer> values = new EnumMap<>(
            RuntimeSettingKey.class);
    private boolean dryRun;
    private Long expectedRevision;

    RuntimeTuningPatchBuilder() {
    }

    public RuntimeTuningPatchBuilder maxSegmentsInCache(final Integer value) {
        values.put(RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE, value);
        return this;
    }

    public RuntimeTuningPatchBuilder segmentCacheKeyLimit(
            final Integer value) {
        values.put(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                value);
        return this;
    }

    public RuntimeTuningPatchBuilder legacyImmutableRunLimit(
            final Integer value) {
        values.put(
                RuntimeSettingKey.MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION,
                value);
        return this;
    }

    public RuntimeTuningPatchBuilder maxNumberOfImmutableRunsPerPartition(
            final Integer value) {
        return legacyImmutableRunLimit(value);
    }

    public RuntimeTuningPatchBuilder segmentSplitKeyThreshold(
            final Integer value) {
        values.put(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                value);
        return this;
    }

    public RuntimeTuningPatchBuilder writePath(
            final Consumer<RuntimeWritePathTuningPatchBuilder> customizer) {
        Vldtn.requireNonNull(customizer, "customizer")
                .accept(new RuntimeWritePathTuningPatchBuilder(values));
        return this;
    }

    public RuntimeTuningPatchBuilder dryRun() {
        this.dryRun = true;
        return this;
    }

    public RuntimeTuningPatchBuilder dryRun(final boolean value) {
        this.dryRun = value;
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
        return new RuntimeTuningPatch(values, dryRun, expectedRevision);
    }
}
