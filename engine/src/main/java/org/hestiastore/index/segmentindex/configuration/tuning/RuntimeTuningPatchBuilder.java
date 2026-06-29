package org.hestiastore.index.segmentindex.configuration.tuning;

import java.util.EnumMap;

/**
 * Fluent builder for {@link RuntimeTuningPatch}.
 */
public final class RuntimeTuningPatchBuilder {

    private final EnumMap<RuntimeTuningKey, RuntimeTuningValue> values =
            new EnumMap<>(RuntimeTuningKey.class);
    private Long expectedRevision;

    RuntimeTuningPatchBuilder() {
    }

    /**
     * Sets the segment key-cache limit.
     *
     * @param value key-cache limit
     * @return this builder
     */
    public RuntimeTuningPatchBuilder cacheKeyLimit(final int value) {
        values.put(RuntimeTuningKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE,
                RuntimeTuningValue.ofInt(value));
        return this;
    }

    /**
     * Sets the cached segment limit.
     *
     * @param value cached segment limit
     * @return this builder
     */
    public RuntimeTuningPatchBuilder cachedSegmentLimit(final int value) {
        values.put(RuntimeTuningKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE,
                RuntimeTuningValue.ofInt(value));
        return this;
    }

    /**
     * Sets the active segment write-cache key limit.
     *
     * @param value write-cache key limit
     * @return this builder
     */
    public RuntimeTuningPatchBuilder segmentWriteCacheKeyLimit(
            final int value) {
        values.put(RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT,
                RuntimeTuningValue.ofInt(value));
        return this;
    }

    /**
     * Sets the maintenance write-cache key limit.
     *
     * @param value maintenance write-cache key limit
     * @return this builder
     */
    public RuntimeTuningPatchBuilder segmentWriteCacheKeyLimitDuringMaintenance(
            final int value) {
        values.put(
                RuntimeTuningKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE,
                RuntimeTuningValue.ofInt(value));
        return this;
    }

    /**
     * Sets the index-level buffered write key limit.
     *
     * @param value buffered write key limit
     * @return this builder
     */
    public RuntimeTuningPatchBuilder indexBufferedWriteKeyLimit(
            final int value) {
        values.put(RuntimeTuningKey.INDEX_BUFFERED_WRITE_KEY_LIMIT,
                RuntimeTuningValue.ofInt(value));
        return this;
    }

    /**
     * Sets the segment split key threshold.
     *
     * @param value split key threshold
     * @return this builder
     */
    public RuntimeTuningPatchBuilder segmentSplitKeyThreshold(
            final int value) {
        values.put(RuntimeTuningKey.SEGMENT_SPLIT_KEY_THRESHOLD,
                RuntimeTuningValue.ofInt(value));
        return this;
    }

    /**
     * Sets the parsed chunk-store cache page limit.
     *
     * @param value page limit
     * @return this builder
     */
    public RuntimeTuningPatchBuilder chunkStoreCachePageLimit(
            final int value) {
        values.put(RuntimeTuningKey.CHUNK_STORE_CACHE_PAGE_LIMIT,
                RuntimeTuningValue.ofInt(value));
        return this;
    }

    /**
     * Sets the expected runtime tuning revision.
     *
     * @param value expected revision
     * @return this builder
     */
    public RuntimeTuningPatchBuilder expectedRevision(final long value) {
        this.expectedRevision = Long.valueOf(value);
        return this;
    }

    /**
     * Sets the expected runtime tuning revision, or clears it when null.
     *
     * @param value expected revision, or null
     * @return this builder
     */
    public RuntimeTuningPatchBuilder expectedRevision(final Long value) {
        this.expectedRevision = value;
        return this;
    }

    /**
     * Builds the runtime tuning patch.
     *
     * @return runtime tuning patch
     */
    public RuntimeTuningPatch build() {
        return new RuntimeTuningPatch(values, expectedRevision);
    }
}
