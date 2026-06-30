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
    private volatile Map<RuntimeTuningKey, RuntimeTuningValue> effective;

    private RuntimeTuningState(final String indexName,
            final EnumMap<RuntimeTuningKey, RuntimeTuningValue> baseline) {
        this.indexName = Vldtn.requireNonNull(indexName, "indexName");
        this.baseline = Vldtn.requireNonNull(baseline, "baseline");
        this.effective = effectiveFromOverrides(overrides);
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
        return RuntimeTuningSnapshot.fromValues(indexName, effective,
                revision.get(), Instant.now());
    }

    synchronized RuntimeTuningSnapshot snapshotOriginal() {
        return RuntimeTuningSnapshot.fromValues(indexName, baseline,
                revision.get(), Instant.now());
    }

    synchronized RuntimeTuningSnapshot apply(
            final Map<RuntimeTuningKey, RuntimeTuningValue> patchValues) {
        final EnumMap<RuntimeTuningKey, RuntimeTuningValue> nextOverrides =
                new EnumMap<>(RuntimeTuningKey.class);
        nextOverrides.putAll(overrides);
        nextOverrides.putAll(patchValues);
        final EnumMap<RuntimeTuningKey, RuntimeTuningValue> nextEffective =
                effectiveFromOverrides(nextOverrides);
        final long nextRevision = revision.incrementAndGet();
        overrides.clear();
        overrides.putAll(nextOverrides);
        effective = nextEffective;
        return RuntimeTuningSnapshot.fromValues(indexName, nextEffective,
                nextRevision, Instant.now());
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

    RuntimeTuningValue effectiveValue(final RuntimeTuningKey key) {
        return effective.get(key);
    }

    /**
     * Returns the effective maximum number of cached segments.
     *
     * @return effective cached segment limit
     */
    public int cachedSegmentLimit() {
        return effectiveValue(RuntimeTuningKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE)
                .asInt();
    }

    /**
     * Returns the effective segment cache key limit.
     *
     * @return effective segment cache key limit
     */
    public int cacheKeyLimit() {
        return effectiveValue(
                RuntimeTuningKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE).asInt();
    }

    /**
     * Returns the effective active segment write cache key limit.
     *
     * @return effective active segment write cache key limit
     */
    public int segmentWriteCacheKeyLimit() {
        return effectiveValue(RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT)
                .asInt();
    }

    /**
     * Returns the effective maintenance segment write cache key limit.
     *
     * @return effective maintenance segment write cache key limit
     */
    public int segmentWriteCacheKeyLimitDuringMaintenance() {
        return effectiveValue(
                RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE)
                .asInt();
    }

    /**
     * Returns the effective index buffered write key limit.
     *
     * @return effective index buffered write key limit
     */
    public int indexBufferedWriteKeyLimit() {
        return effectiveValue(RuntimeTuningKey.INDEX_BUFFERED_WRITE_KEY_LIMIT)
                .asInt();
    }

    /**
     * Returns the effective segment split key threshold.
     *
     * @return effective segment split key threshold
     */
    public int segmentSplitKeyThreshold() {
        return effectiveValue(RuntimeTuningKey.SEGMENT_SPLIT_KEY_THRESHOLD)
                .asInt();
    }

    /**
     * Returns the effective chunk store cache page limit.
     *
     * @return effective chunk store cache page limit
     */
    public int chunkStoreCachePageLimit() {
        return effectiveValue(RuntimeTuningKey.CHUNK_STORE_CACHE_PAGE_LIMIT)
                .asInt();
    }

    long revision() {
        return revision.get();
    }

    private EnumMap<RuntimeTuningKey, RuntimeTuningValue> effectiveFromOverrides(
            final Map<RuntimeTuningKey, RuntimeTuningValue> overrideValues) {
        final EnumMap<RuntimeTuningKey, RuntimeTuningValue> effectiveValues =
                new EnumMap<>(RuntimeTuningKey.class);
        effectiveValues.putAll(baseline);
        effectiveValues.putAll(overrideValues);
        return effectiveValues;
    }
}
