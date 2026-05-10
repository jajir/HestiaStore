package org.hestiastore.index.segmentindex.configuration.tuning;

import java.time.Instant;
import java.util.Map;

import org.hestiastore.index.Vldtn;

/**
 * Immutable runtime-tuning snapshot.
 */
public final class RuntimeTuningSnapshot {

    private final String indexName;
    private final long revision;
    private final Instant capturedAt;
    private final RuntimeSegmentTuningSnapshot segment;
    private final RuntimeWritePathTuningSnapshot writePath;

    RuntimeTuningSnapshot(final String indexName, final long revision,
            final Instant capturedAt,
            final RuntimeSegmentTuningSnapshot segment,
            final RuntimeWritePathTuningSnapshot writePath) {
        this.indexName = Vldtn.requireNotBlank(indexName, "indexName");
        this.revision = Vldtn.requireGreaterThanOrEqualToZero(revision,
                "revision");
        this.capturedAt = Vldtn.requireNonNull(capturedAt, "capturedAt");
        this.segment = Vldtn.requireNonNull(segment, "segment");
        this.writePath = Vldtn.requireNonNull(writePath, "writePath");
    }

    static RuntimeTuningSnapshot fromValues(final String indexName,
            final Map<RuntimeSettingKey, RuntimeTuningValue> values,
            final long revision,
            final Instant capturedAt) {
        final Map<RuntimeSettingKey, RuntimeTuningValue> input = Vldtn
                .requireNonNull(values, "values");
        return new RuntimeTuningSnapshot(indexName, revision, capturedAt,
                new RuntimeSegmentTuningSnapshot(
                        value(input,
                                RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE)
                                .asInt(),
                        value(input,
                                RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE)
                                .asInt()),
                new RuntimeWritePathTuningSnapshot(
                        value(input,
                                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT)
                                .asInt(),
                        value(input,
                                RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE)
                                .asInt(),
                        value(input,
                                RuntimeSettingKey.INDEX_BUFFERED_WRITE_KEY_LIMIT)
                                .asInt(),
                        value(input,
                                RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD)
                                .asInt()));
    }

    public String indexName() {
        return indexName;
    }

    public long revision() {
        return revision;
    }

    public Instant capturedAt() {
        return capturedAt;
    }

    public RuntimeSegmentTuningSnapshot segment() {
        return segment;
    }

    public RuntimeWritePathTuningSnapshot writePath() {
        return writePath;
    }

    RuntimeTuningValue value(final RuntimeSettingKey key) {
        return switch (key) {
            case MAX_NUMBER_OF_SEGMENTS_IN_CACHE -> RuntimeTuningValue
                    .ofInt(segment.cachedSegmentLimit());
            case MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE -> RuntimeTuningValue
                    .ofInt(segment.cacheKeyLimit());
            case SEGMENT_WRITE_CACHE_KEY_LIMIT -> RuntimeTuningValue
                    .ofInt(writePath.segmentWriteCacheKeyLimit());
            case SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE -> RuntimeTuningValue
                    .ofInt(writePath
                            .segmentWriteCacheKeyLimitDuringMaintenance());
            case INDEX_BUFFERED_WRITE_KEY_LIMIT -> RuntimeTuningValue
                    .ofInt(writePath.indexBufferedWriteKeyLimit());
            case SEGMENT_SPLIT_KEY_THRESHOLD -> RuntimeTuningValue
                    .ofInt(writePath.segmentSplitKeyThreshold());
        };
    }

    private static RuntimeTuningValue value(
            final Map<RuntimeSettingKey, RuntimeTuningValue> values,
            final RuntimeSettingKey key) {
        return Vldtn.requireNonNull(values.get(key), key.name());
    }
}
