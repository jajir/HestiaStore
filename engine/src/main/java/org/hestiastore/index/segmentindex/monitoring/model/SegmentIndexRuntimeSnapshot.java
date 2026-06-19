package org.hestiastore.index.segmentindex.monitoring.model;

import java.time.Instant;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Immutable runtime snapshot for one index.
 */
public final class SegmentIndexRuntimeSnapshot {

    private final String indexName;
    private final SegmentIndexState state;
    private final Instant capturedAt;
    private final SegmentIndexOperationMetrics operations;
    private final SegmentIndexRegistryCacheMetrics registryCache;
    private final SegmentIndexChunkStoreCacheMetrics chunkStoreCache;
    private final SegmentIndexSegmentMetrics segments;
    private final SegmentIndexWritePathMetrics writePath;
    private final SegmentIndexMaintenanceMetrics maintenance;
    private final SegmentIndexSplitMetrics split;
    private final SegmentIndexLatencyMetrics latency;
    private final SegmentIndexBloomFilterMetrics bloomFilter;
    private final SegmentIndexWalMetrics wal;

    /**
     * Creates a runtime snapshot.
     *
     * @param indexName logical index name
     * @param state current index lifecycle state
     * @param capturedAt capture timestamp
     * @param operations operation metrics
     * @param registryCache registry cache metrics
     * @param chunkStoreCache chunk-store cache metrics
     * @param segments segment metrics
     * @param writePath write-path metrics
     * @param maintenance maintenance metrics
     * @param split split metrics
     * @param latency latency metrics
     * @param bloomFilter Bloom filter metrics
     * @param wal WAL metrics
     */
    @SuppressWarnings("java:S107")
    public SegmentIndexRuntimeSnapshot(final String indexName,
            final SegmentIndexState state, final Instant capturedAt,
            final SegmentIndexOperationMetrics operations,
            final SegmentIndexRegistryCacheMetrics registryCache,
            final SegmentIndexChunkStoreCacheMetrics chunkStoreCache,
            final SegmentIndexSegmentMetrics segments,
            final SegmentIndexWritePathMetrics writePath,
            final SegmentIndexMaintenanceMetrics maintenance,
            final SegmentIndexSplitMetrics split,
            final SegmentIndexLatencyMetrics latency,
            final SegmentIndexBloomFilterMetrics bloomFilter,
            final SegmentIndexWalMetrics wal) {
        this.indexName = Vldtn.requireNotBlank(indexName, "indexName");
        this.state = Vldtn.requireNonNull(state, "state");
        this.capturedAt = Vldtn.requireNonNull(capturedAt, "capturedAt");
        this.operations = Vldtn.requireNonNull(operations, "operations");
        this.registryCache = Vldtn.requireNonNull(registryCache,
                "registryCache");
        this.chunkStoreCache = Vldtn.requireNonNull(chunkStoreCache,
                "chunkStoreCache");
        this.segments = Vldtn.requireNonNull(segments, "segments");
        this.writePath = Vldtn.requireNonNull(writePath, "writePath");
        this.maintenance = Vldtn.requireNonNull(maintenance, "maintenance");
        this.split = Vldtn.requireNonNull(split, "split");
        this.latency = Vldtn.requireNonNull(latency, "latency");
        this.bloomFilter = Vldtn.requireNonNull(bloomFilter, "bloomFilter");
        this.wal = Vldtn.requireNonNull(wal, "wal");
    }

    /**
     * Returns the logical index name.
     *
     * @return index name
     */
    public String indexName() {
        return indexName;
    }

    /**
     * Returns the current lifecycle state.
     *
     * @return lifecycle state
     */
    public SegmentIndexState state() {
        return state;
    }

    /**
     * Returns the capture timestamp.
     *
     * @return capture timestamp
     */
    public Instant capturedAt() {
        return capturedAt;
    }

    /**
     * Returns operation metrics.
     *
     * @return operation metrics
     */
    public SegmentIndexOperationMetrics operations() {
        return operations;
    }

    /**
     * Returns registry cache metrics.
     *
     * @return registry cache metrics
     */
    public SegmentIndexRegistryCacheMetrics registryCache() {
        return registryCache;
    }

    /**
     * Returns chunk-store cache metrics.
     *
     * @return chunk-store cache metrics
     */
    public SegmentIndexChunkStoreCacheMetrics chunkStoreCache() {
        return chunkStoreCache;
    }

    /**
     * Returns segment metrics.
     *
     * @return segment metrics
     */
    public SegmentIndexSegmentMetrics segments() {
        return segments;
    }

    /**
     * Returns write-path metrics.
     *
     * @return write-path metrics
     */
    public SegmentIndexWritePathMetrics writePath() {
        return writePath;
    }

    /**
     * Returns maintenance metrics.
     *
     * @return maintenance metrics
     */
    public SegmentIndexMaintenanceMetrics maintenance() {
        return maintenance;
    }

    /**
     * Returns split metrics.
     *
     * @return split metrics
     */
    public SegmentIndexSplitMetrics split() {
        return split;
    }

    /**
     * Returns latency metrics.
     *
     * @return latency metrics
     */
    public SegmentIndexLatencyMetrics latency() {
        return latency;
    }

    /**
     * Returns Bloom filter metrics.
     *
     * @return Bloom filter metrics
     */
    public SegmentIndexBloomFilterMetrics bloomFilter() {
        return bloomFilter;
    }

    /**
     * Returns WAL metrics.
     *
     * @return WAL metrics
     */
    public SegmentIndexWalMetrics wal() {
        return wal;
    }
}
