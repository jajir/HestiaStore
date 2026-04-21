package org.hestiastore.index.segmentindex.core.control;

import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.control.model.ConfigurationSnapshot;
import org.hestiastore.index.control.model.RuntimeSettingKey;
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
        final EnumMap<RuntimeSettingKey, Integer> baselineValues = new EnumMap<>(
                RuntimeSettingKey.class);
        baselineValues.put(RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                configuration.getMaxNumberOfSegmentsInCache());
        baselineValues.put(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                configuration.getMaxNumberOfKeysInSegmentCache());
        baselineValues.put(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_ACTIVE_PARTITION,
                configuration.getMaxNumberOfKeysInActivePartition());
        baselineValues.put(
                RuntimeSettingKey.MAX_NUMBER_OF_IMMUTABLE_RUNS_PER_PARTITION,
                configuration.getMaxNumberOfImmutableRunsPerPartition());
        baselineValues.put(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BUFFER,
                configuration.getMaxNumberOfKeysInPartitionBuffer());
        baselineValues.put(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_INDEX_BUFFER,
                configuration.getMaxNumberOfKeysInIndexBuffer());
        baselineValues.put(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_PARTITION_BEFORE_SPLIT,
                configuration.getMaxNumberOfKeysInPartitionBeforeSplit());
        return new RuntimeTuningState(configuration.getIndexName(),
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
