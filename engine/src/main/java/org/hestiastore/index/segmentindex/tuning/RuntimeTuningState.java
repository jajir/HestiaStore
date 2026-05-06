package org.hestiastore.index.segmentindex.tuning;

import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.IndexConfiguration;

/**
 * Holds runtime-tunable index settings and exposes immutable snapshots.
 */
public final class RuntimeTuningState {

    private final String indexName;
    private final EnumMap<RuntimeSettingKey, Integer> baseline;
    private final EnumMap<RuntimeSettingKey, Integer> overrides = new EnumMap<>(
            RuntimeSettingKey.class);
    private final AtomicLong revision = new AtomicLong(0L);

    private RuntimeTuningState(final String indexName,
            final EnumMap<RuntimeSettingKey, Integer> baseline) {
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
        this.baseline = Vldtn.requireNonNull(baseline, "baseline");
    }

    public static <K, V> RuntimeTuningState fromConfiguration(
            final IndexConfiguration<K, V> configuration) {
        final var tuning = configuration.runtimeTuning();
        final var writePath = tuning.writePath();
        final EnumMap<RuntimeSettingKey, Integer> baselineValues = new EnumMap<>(
                RuntimeSettingKey.class);
        baselineValues.put(RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                tuning.maxSegmentsInCache());
        baselineValues.put(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                tuning.segmentCacheKeyLimit());
        baselineValues.put(
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT,
                writePath.segmentWriteCacheKeyLimit());
        baselineValues.put(
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                writePath.segmentWriteCacheKeyLimitDuringMaintenance());
        baselineValues.put(
                RuntimeSettingKey.INDEX_BUFFERED_WRITE_KEY_LIMIT,
                writePath.indexBufferedWriteKeyLimit());
        baselineValues.put(
                RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD,
                writePath.segmentSplitKeyThreshold());
        return new RuntimeTuningState(configuration.identity().name(),
                baselineValues);
    }

    public synchronized ConfigurationSnapshot snapshotCurrent() {
        return new ConfigurationSnapshot(indexName,
                effectiveFromOverrides(overrides), revision.get(),
                Instant.now());
    }

    public synchronized ConfigurationSnapshot snapshotOriginal() {
        return new ConfigurationSnapshot(indexName, baseline, revision.get(),
                Instant.now());
    }

    public synchronized ConfigurationSnapshot apply(
            final Map<RuntimeSettingKey, Integer> patchValues) {
        overrides.putAll(patchValues);
        final long nextRevision = revision.incrementAndGet();
        return new ConfigurationSnapshot(indexName,
                effectiveFromOverrides(overrides), nextRevision, Instant.now());
    }

    public synchronized Map<RuntimeSettingKey, Integer> previewEffective(
            final Map<RuntimeSettingKey, Integer> patchValues) {
        final EnumMap<RuntimeSettingKey, Integer> mergedOverrides = new EnumMap<>(
                RuntimeSettingKey.class);
        mergedOverrides.putAll(overrides);
        mergedOverrides.putAll(patchValues);
        return effectiveFromOverrides(mergedOverrides);
    }

    public synchronized int effectiveValue(final RuntimeSettingKey key) {
        final Integer override = overrides.get(key);
        if (override != null) {
            return override.intValue();
        }
        return baseline.get(key).intValue();
    }

    public synchronized long revision() {
        return revision.get();
    }

    private EnumMap<RuntimeSettingKey, Integer> effectiveFromOverrides(
            final Map<RuntimeSettingKey, Integer> overrideValues) {
        final EnumMap<RuntimeSettingKey, Integer> effective = new EnumMap<>(
                RuntimeSettingKey.class);
        effective.putAll(baseline);
        effective.putAll(overrideValues);
        return effective;
    }
}
