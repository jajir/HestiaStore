package org.hestiastore.index.segmentindex.configuration.tuning;

import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfiguration;

/**
 * Holds runtime-tunable index settings and exposes immutable snapshots.
 */
public final class RuntimeTuningState {

    private final String indexName;
    private final EnumMap<RuntimeSettingKey, RuntimeTuningValue> baseline;
    private final EnumMap<RuntimeSettingKey, RuntimeTuningValue> overrides =
            new EnumMap<>(RuntimeSettingKey.class);
    private final AtomicLong revision = new AtomicLong(0L);

    private RuntimeTuningState(final String indexName,
            final EnumMap<RuntimeSettingKey, RuntimeTuningValue> baseline) {
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
        this.baseline = Vldtn.requireNonNull(baseline, "baseline");
    }

    public static <K, V> RuntimeTuningState fromConfiguration(
            final EffectiveIndexConfiguration<K, V> configuration) {
        final var tuning = configuration.runtimeTuning();
        final var writePath = tuning.writePath();
        final EnumMap<RuntimeSettingKey, RuntimeTuningValue> baselineValues =
                new EnumMap<>(RuntimeSettingKey.class);
        baselineValues.put(RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                RuntimeTuningValue.ofInt(tuning.maxSegmentsInCache()));
        baselineValues.put(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                RuntimeTuningValue.ofInt(tuning.segmentCacheKeyLimit()));
        baselineValues.put(
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT,
                RuntimeTuningValue
                        .ofInt(writePath.segmentWriteCacheKeyLimit()));
        baselineValues.put(
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                RuntimeTuningValue.ofInt(writePath
                        .segmentWriteCacheKeyLimitDuringMaintenance()));
        baselineValues.put(
                RuntimeSettingKey.INDEX_BUFFERED_WRITE_KEY_LIMIT,
                RuntimeTuningValue
                        .ofInt(writePath.indexBufferedWriteKeyLimit()));
        baselineValues.put(
                RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD,
                RuntimeTuningValue.ofInt(writePath.segmentSplitKeyThreshold()));
        baselineValues.put(RuntimeSettingKey.CHUNK_STORE_CACHE_PAGE_LIMIT,
                RuntimeTuningValue
                        .ofInt(tuning.chunkStoreCache().pageLimit()));
        return new RuntimeTuningState(configuration.identity().name(),
                baselineValues);
    }

    synchronized RuntimeTuningSnapshot snapshotCurrent() {
        return RuntimeTuningSnapshot.fromValues(indexName,
                effectiveFromOverrides(overrides), revision.get(), Instant.now());
    }

    synchronized RuntimeTuningSnapshot snapshotOriginal() {
        return RuntimeTuningSnapshot.fromValues(indexName, baseline,
                revision.get(), Instant.now());
    }

    synchronized RuntimeTuningSnapshot apply(
            final Map<RuntimeSettingKey, RuntimeTuningValue> patchValues) {
        overrides.putAll(patchValues);
        final long nextRevision = revision.incrementAndGet();
        return RuntimeTuningSnapshot.fromValues(indexName,
                effectiveFromOverrides(overrides), nextRevision, Instant.now());
    }

    synchronized Map<RuntimeSettingKey, RuntimeTuningValue> previewEffective(
            final Map<RuntimeSettingKey, RuntimeTuningValue> patchValues) {
        final EnumMap<RuntimeSettingKey, RuntimeTuningValue> mergedOverrides =
                new EnumMap<>(RuntimeSettingKey.class);
        mergedOverrides.putAll(overrides);
        mergedOverrides.putAll(patchValues);
        return effectiveFromOverrides(mergedOverrides);
    }

    synchronized RuntimeTuningSnapshot previewSnapshot(
            final Map<RuntimeSettingKey, RuntimeTuningValue> patchValues) {
        return RuntimeTuningSnapshot.fromValues(indexName,
                previewEffective(patchValues), revision.get(), Instant.now());
    }

    synchronized RuntimeTuningValue effectiveValue(final RuntimeSettingKey key) {
        final RuntimeTuningValue override = overrides.get(key);
        if (override != null) {
            return override;
        }
        return baseline.get(key);
    }

    public synchronized int cachedSegmentLimit() {
        return effectiveValue(RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE)
                .asInt();
    }

    public synchronized int cacheKeyLimit() {
        return effectiveValue(
                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE).asInt();
    }

    public synchronized int segmentWriteCacheKeyLimit() {
        return effectiveValue(RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT)
                .asInt();
    }

    public synchronized int segmentWriteCacheKeyLimitDuringMaintenance() {
        return effectiveValue(
                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE)
                        .asInt();
    }

    public synchronized int indexBufferedWriteKeyLimit() {
        return effectiveValue(RuntimeSettingKey.INDEX_BUFFERED_WRITE_KEY_LIMIT)
                .asInt();
    }

    public synchronized int segmentSplitKeyThreshold() {
        return effectiveValue(RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD)
                .asInt();
    }

    public synchronized int chunkStoreCachePageLimit() {
        return effectiveValue(RuntimeSettingKey.CHUNK_STORE_CACHE_PAGE_LIMIT)
                .asInt();
    }

    synchronized long revision() {
        return revision.get();
    }

    private EnumMap<RuntimeSettingKey, RuntimeTuningValue> effectiveFromOverrides(
            final Map<RuntimeSettingKey, RuntimeTuningValue> overrideValues) {
        final EnumMap<RuntimeSettingKey, RuntimeTuningValue> effective =
                new EnumMap<>(RuntimeSettingKey.class);
        effective.putAll(baseline);
        effective.putAll(overrideValues);
        return effective;
    }
}
