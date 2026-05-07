package org.hestiastore.index.segmentindex.tuning;

import java.time.Instant;
import java.util.Map;

import org.hestiastore.index.Vldtn;

/**
 * Typed runtime-tuning snapshot view.
 */
public final class RuntimeTuningSnapshot {

    private final ConfigurationSnapshot snapshot;
    private final RuntimeWritePathTuningSnapshot writePath;

    private RuntimeTuningSnapshot(final ConfigurationSnapshot snapshot) {
        this.snapshot = Vldtn.requireNonNull(snapshot, "snapshot");
        this.writePath = new RuntimeWritePathTuningSnapshot(
                value(RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT),
                value(RuntimeSettingKey.SEGMENT_WRITE_CACHE_KEY_LIMIT_DURING_MAINTENANCE),
                value(RuntimeSettingKey.INDEX_BUFFERED_WRITE_KEY_LIMIT),
                value(RuntimeSettingKey.SEGMENT_SPLIT_KEY_THRESHOLD));
    }

    /**
     * Creates typed snapshot from existing enum-map snapshot.
     *
     * @param snapshot configuration snapshot
     * @return typed runtime-tuning snapshot
     */
    public static RuntimeTuningSnapshot from(
            final ConfigurationSnapshot snapshot) {
        return new RuntimeTuningSnapshot(snapshot);
    }

    public ConfigurationSnapshot toConfigurationSnapshot() {
        return snapshot;
    }

    public String indexName() {
        return snapshot.getIndexName();
    }

    public String getIndexName() {
        return snapshot.getIndexName();
    }

    public long revision() {
        return snapshot.getRevision();
    }

    public long getRevision() {
        return snapshot.getRevision();
    }

    public Instant capturedAt() {
        return snapshot.getCapturedAt();
    }

    public Instant getCapturedAt() {
        return snapshot.getCapturedAt();
    }

    public Integer maxSegmentsInCache() {
        return value(RuntimeSettingKey.MAX_NUMBER_OF_SEGMENTS_IN_CACHE);
    }

    public Integer getMaxSegmentsInCache() {
        return maxSegmentsInCache();
    }

    public Integer segmentCacheKeyLimit() {
        return value(RuntimeSettingKey.MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE);
    }

    public Integer getSegmentCacheKeyLimit() {
        return segmentCacheKeyLimit();
    }

    public RuntimeWritePathTuningSnapshot writePath() {
        return writePath;
    }

    public RuntimeWritePathTuningSnapshot getWritePath() {
        return writePath;
    }

    private Integer value(final RuntimeSettingKey key) {
        final Map<RuntimeSettingKey, Integer> values = snapshot.getValues();
        final Integer value = values.get(key);
        return Vldtn.requireNonNull(value, key.name());
    }
}
