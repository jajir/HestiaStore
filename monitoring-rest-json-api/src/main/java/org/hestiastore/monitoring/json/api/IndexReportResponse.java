package org.hestiastore.monitoring.json.api;

import java.beans.ConstructorProperties;
import java.util.Objects;

/**
 * Per-index grouped metrics section inside node report payload.
 */
@SuppressWarnings("java:S6206")
public final class IndexReportResponse {

    private final String indexName;
    private final String state;
    private final boolean ready;
    private final OperationReportResponse operations;
    private final RegistryCacheReportResponse registryCache;
    private final ChunkStoreCacheReportResponse chunkStoreCache;
    private final SegmentReportResponse segments;
    private final WritePathReportResponse writePath;
    private final MaintenanceReportResponse maintenance;
    private final SplitReportResponse split;
    private final LatencyReportResponse latency;
    private final BloomFilterReportResponse bloomFilter;
    private final WalReportResponse wal;

    /**
     * Creates validated per-index metrics payload.
     *
     * @param indexName logical index name
     * @param state lifecycle state name
     * @param ready whether the index is ready
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
    @ConstructorProperties({ "indexName", "state", "ready", "operations",
            "registryCache", "chunkStoreCache", "segments", "writePath",
            "maintenance", "split", "latency", "bloomFilter", "wal" })
    @SuppressWarnings("java:S107")
    public IndexReportResponse(final String indexName, final String state,
            final boolean ready, final OperationReportResponse operations,
            final RegistryCacheReportResponse registryCache,
            final ChunkStoreCacheReportResponse chunkStoreCache,
            final SegmentReportResponse segments,
            final WritePathReportResponse writePath,
            final MaintenanceReportResponse maintenance,
            final SplitReportResponse split,
            final LatencyReportResponse latency,
            final BloomFilterReportResponse bloomFilter,
            final WalReportResponse wal) {
        this.indexName = normalize(indexName, "indexName");
        this.state = normalize(state, "state");
        this.ready = ready;
        this.operations = Objects.requireNonNull(operations, "operations");
        this.registryCache = Objects.requireNonNull(registryCache,
                "registryCache");
        this.chunkStoreCache = Objects.requireNonNull(chunkStoreCache,
                "chunkStoreCache");
        this.segments = Objects.requireNonNull(segments, "segments");
        this.writePath = Objects.requireNonNull(writePath, "writePath");
        this.maintenance = Objects.requireNonNull(maintenance, "maintenance");
        this.split = Objects.requireNonNull(split, "split");
        this.latency = Objects.requireNonNull(latency, "latency");
        this.bloomFilter = Objects.requireNonNull(bloomFilter, "bloomFilter");
        this.wal = Objects.requireNonNull(wal, "wal");
    }

    private static String normalize(final String value, final String name) {
        final String normalized = Objects.requireNonNull(value, name).trim();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return normalized;
    }

    /**
     * Returns logical index name.
     *
     * @return logical index name
     */
    public String indexName() {
        return indexName;
    }

    /**
     * Returns lifecycle state name.
     *
     * @return lifecycle state name
     */
    public String state() {
        return state;
    }

    /**
     * Returns whether the index is ready.
     *
     * @return true when the index is ready
     */
    public boolean ready() {
        return ready;
    }

    /**
     * Returns operation metrics.
     *
     * @return operation metrics
     */
    public OperationReportResponse operations() {
        return operations;
    }

    /**
     * Returns registry cache metrics.
     *
     * @return registry cache metrics
     */
    public RegistryCacheReportResponse registryCache() {
        return registryCache;
    }

    /**
     * Returns chunk-store cache metrics.
     *
     * @return chunk-store cache metrics
     */
    public ChunkStoreCacheReportResponse chunkStoreCache() {
        return chunkStoreCache;
    }

    /**
     * Returns segment metrics.
     *
     * @return segment metrics
     */
    public SegmentReportResponse segments() {
        return segments;
    }

    /**
     * Returns write-path metrics.
     *
     * @return write-path metrics
     */
    public WritePathReportResponse writePath() {
        return writePath;
    }

    /**
     * Returns maintenance metrics.
     *
     * @return maintenance metrics
     */
    public MaintenanceReportResponse maintenance() {
        return maintenance;
    }

    /**
     * Returns split metrics.
     *
     * @return split metrics
     */
    public SplitReportResponse split() {
        return split;
    }

    /**
     * Returns latency metrics.
     *
     * @return latency metrics
     */
    public LatencyReportResponse latency() {
        return latency;
    }

    /**
     * Returns Bloom filter metrics.
     *
     * @return Bloom filter metrics
     */
    public BloomFilterReportResponse bloomFilter() {
        return bloomFilter;
    }

    /**
     * Returns WAL metrics.
     *
     * @return WAL metrics
     */
    public WalReportResponse wal() {
        return wal;
    }
}
