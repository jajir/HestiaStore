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
    private final EnumMap<RuntimeTuningKey, RuntimeTuningValue> baseline;
    private final EnumMap<RuntimeTuningKey, RuntimeTuningValue> overrides =
            new EnumMap<>(RuntimeTuningKey.class);
    private final AtomicLong revision = new AtomicLong(0L);

    private RuntimeTuningState(final String indexName,
            final EnumMap<RuntimeTuningKey, RuntimeTuningValue> baseline) {
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
        this.baseline = Vldtn.requireNonNull(baseline, "baseline");
    }

    public static <K, V> RuntimeTuningState fromConfiguration(
            final EffectiveIndexConfiguration<K, V> configuration) {
        final var tuning = configuration.runtimeTuning();
        final var writePath = tuning.writePath();
        final EnumMap<RuntimeTuningKey, RuntimeTuningValue> baselineValues =
                new EnumMap<>(RuntimeTuningKey.class);
        baselineValues.put(RuntimeTuningKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                RuntimeTuningValue.ofInt(tuning.maxSegmentsInCache()));
        baselineValues.put(
                RuntimeTuningKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                RuntimeTuningValue.ofInt(tuning.segmentCacheKeyLimit()));
        baselineValues.put(
                RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT,
                RuntimeTuningValue
                        .ofInt(writePath.segmentWriteCacheKeyLimit()));
        baselineValues.put(
                RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                RuntimeTuningValue.ofInt(writePath
                        .segmentWriteCacheKeyLimitDuringMaintenance()));
        baselineValues.put(
                RuntimeTuningKey.INDEX_BUFFERED_WRITE_KEY_LIMIT,
                RuntimeTuningValue
                        .ofInt(writePath.indexBufferedWriteKeyLimit()));
        baselineValues.put(
                RuntimeTuningKey.SEGMENT_SPLIT_KEY_THRESHOLD,
                RuntimeTuningValue.ofInt(writePath.segmentSplitKeyThreshold()));
        baselineValues.put(RuntimeTuningKey.CHUNK_STORE_CACHE_PAGE_LIMIT,
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
            final Map<RuntimeTuningKey, RuntimeTuningValue> patchValues) {
        overrides.putAll(patchValues);
        final long nextRevision = revision.incrementAndGet();
        return RuntimeTuningSnapshot.fromValues(indexName,
                effectiveFromOverrides(overrides), nextRevision, Instant.now());
    }

    synchronized Map<RuntimeTuningKey, RuntimeTuningValue> previewEffective(
            final Map<RuntimeTuningKey, RuntimeTuningValue> patchValues) {
        final EnumMap<RuntimeTuningKey, RuntimeTuningValue> mergedOverrides =
                new EnumMap<>(RuntimeTuningKey.class);
        mergedOverrides.putAll(overrides);
        mergedOverrides.putAll(patchValues);
        return effectiveFromOverrides(mergedOverrides);
    }

    synchronized RuntimeTuningSnapshot previewSnapshot(
            final Map<RuntimeTuningKey, RuntimeTuningValue> patchValues) {
        return RuntimeTuningSnapshot.fromValues(indexName,
                previewEffective(patchValues), revision.get(), Instant.now());
    }

    synchronized RuntimeTuningValue effectiveValue(final RuntimeTuningKey key) {
        final RuntimeTuningValue override = overrides.get(key);
        if (override != null) {
            return override;
        }
        return baseline.get(key);
    }

    public synchronized int cachedSegmentLimit() {
        return effectiveValue(RuntimeTuningKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE)
                .asInt();
    }

    public synchronized int cacheKeyLimit() {
        return effectiveValue(
                RuntimeTuningKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE).asInt();
    }

    public synchronized int segmentWriteCacheKeyLimit() {
        return effectiveValue(RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT)
                .asInt();
    }

    public synchronized int segmentWriteCacheKeyLimitDuringMaintenance() {
        return effectiveValue(
                RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE)
                        .asInt();
    }

    public synchronized int indexBufferedWriteKeyLimit() {
        return effectiveValue(RuntimeTuningKey.INDEX_BUFFERED_WRITE_KEY_LIMIT)
                .asInt();
    }

    public synchronized int segmentSplitKeyThreshold() {
        return effectiveValue(RuntimeTuningKey.SEGMENT_SPLIT_KEY_THRESHOLD)
                .asInt();
    }

    public synchronized int chunkStoreCachePageLimit() {
        return effectiveValue(RuntimeTuningKey.CHUNK_STORE_CACHE_PAGE_LIMIT)
                .asInt();
    }

    synchronized long revision() {
        return revision.get();
    }

    private EnumMap<RuntimeTuningKey, RuntimeTuningValue> effectiveFromOverrides(
            final Map<RuntimeTuningKey, RuntimeTuningValue> overrideValues) {
        final EnumMap<RuntimeTuningKey, RuntimeTuningValue> effective =
                new EnumMap<>(RuntimeTuningKey.class);
        effective.putAll(baseline);
        effective.putAll(overrideValues);
        return effective;
    }
}
