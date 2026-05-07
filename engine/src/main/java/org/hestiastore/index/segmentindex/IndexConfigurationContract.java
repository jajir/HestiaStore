package org.hestiastore.index.segmentindex;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterSpec;
import org.hestiastore.index.chunkstore.ChunkFilterSpecs;

/**
 * Supplies default values for index configuration sections.
 */
public interface IndexConfigurationContract {
    int DEFAULT_SEGMENT_MAX_KEYS = 10_000_000;
    int DEFAULT_SEGMENT_SPLIT_KEY_THRESHOLD = 10_000_000;
    int DEFAULT_SEGMENT_CACHE_KEY_LIMIT = 10_000;
    int DEFAULT_SEGMENT_CHUNK_KEY_LIMIT = 1_000;
    int DEFAULT_CACHED_SEGMENT_LIMIT = 10;
    int DEFAULT_DELTA_CACHE_FILE_LIMIT = 10;

    int DEFAULT_BLOOM_FILTER_HASH_FUNCTIONS = 3;
    int DEFAULT_BLOOM_FILTER_INDEX_SIZE_BYTES = 5_000_000;
    double DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_PROBABILITY = 0.01;

    int DEFAULT_DISK_IO_BUFFER_SIZE_BYTES = 1024 * 8;
    int DEFAULT_SEGMENT_MAINTENANCE_THREADS = 10;
    int DEFAULT_INDEX_MAINTENANCE_THREADS = 10;
    int DEFAULT_REGISTRY_LIFECYCLE_THREADS = 3;
    int DEFAULT_INDEX_BUSY_BACKOFF_MILLIS = 5;
    int DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS = 30_000;
    boolean DEFAULT_BACKGROUND_MAINTENANCE_AUTO_ENABLED = true;

    /**
     * Returns default segment sizing and cache settings.
     *
     * @return default segment section
     */
    default IndexSegmentConfiguration segment() {
        return new IndexSegmentConfiguration(DEFAULT_SEGMENT_MAX_KEYS,
                DEFAULT_SEGMENT_CHUNK_KEY_LIMIT,
                DEFAULT_SEGMENT_CACHE_KEY_LIMIT, DEFAULT_CACHED_SEGMENT_LIMIT,
                DEFAULT_DELTA_CACHE_FILE_LIMIT);
    }

    /**
     * Returns default direct-to-segment write-path settings.
     *
     * @return default write-path section
     */
    default IndexWritePathConfiguration writePath() {
        final IndexSegmentConfiguration segment = segment();
        final int segmentWriteCacheKeyLimit =
                Math.max(1, segment.cacheKeyLimit().intValue() / 2);
        final int maintenanceWriteCacheKeyLimit =
                Math.max(segmentWriteCacheKeyLimit + 1,
                        (int) Math.ceil(segmentWriteCacheKeyLimit * 1.4));
        final int indexBufferedWriteKeyLimit = Math.max(
                maintenanceWriteCacheKeyLimit,
                maintenanceWriteCacheKeyLimit
                        * segment.cachedSegmentLimit().intValue());
        return new IndexWritePathConfiguration(segmentWriteCacheKeyLimit,
                maintenanceWriteCacheKeyLimit, indexBufferedWriteKeyLimit,
                DEFAULT_SEGMENT_SPLIT_KEY_THRESHOLD);
    }

    /**
     * Returns default runtime-tunable settings.
     *
     * @return default runtime tuning section
     */
    default IndexRuntimeTuningConfiguration runtimeTuning() {
        final IndexSegmentConfiguration segment = segment();
        return new IndexRuntimeTuningConfiguration(
                segment.cachedSegmentLimit(), segment.cacheKeyLimit(),
                writePath());
    }

    /**
     * Returns default Bloom filter settings.
     *
     * @return default Bloom filter section
     */
    default IndexBloomFilterConfiguration bloomFilter() {
        return new IndexBloomFilterConfiguration(
                DEFAULT_BLOOM_FILTER_HASH_FUNCTIONS,
                DEFAULT_BLOOM_FILTER_INDEX_SIZE_BYTES,
                DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_PROBABILITY);
    }

    /**
     * Returns default disk I/O settings.
     *
     * @return default I/O section
     */
    default IndexIoConfiguration io() {
        return new IndexIoConfiguration(DEFAULT_DISK_IO_BUFFER_SIZE_BYTES);
    }

    /**
     * Returns default maintenance, lifecycle, and retry settings.
     *
     * @return default maintenance section
     */
    default IndexMaintenanceConfiguration maintenance() {
        return new IndexMaintenanceConfiguration(
                DEFAULT_SEGMENT_MAINTENANCE_THREADS,
                DEFAULT_INDEX_MAINTENANCE_THREADS,
                DEFAULT_REGISTRY_LIFECYCLE_THREADS,
                DEFAULT_INDEX_BUSY_BACKOFF_MILLIS,
                DEFAULT_INDEX_BUSY_TIMEOUT_MILLIS,
                DEFAULT_BACKGROUND_MAINTENANCE_AUTO_ENABLED);
    }

    /**
     * Returns default logging settings.
     *
     * @return default logging section
     */
    default IndexLoggingConfiguration logging() {
        return new IndexLoggingConfiguration(Boolean.TRUE);
    }

    /**
     * Returns default WAL configuration.
     *
     * @return default WAL settings
     */
    default IndexWalConfiguration wal() {
        return IndexWalConfiguration.EMPTY;
    }

    /**
     * Returns default persisted chunk filter pipeline settings.
     *
     * @return default filter section
     */
    default IndexFilterConfiguration filters() {
        return new IndexFilterConfiguration(defaultEncodingChunkFilterSpecs(),
                defaultDecodingChunkFilterSpecs());
    }

    private static List<ChunkFilterSpec> defaultEncodingChunkFilterSpecs() {
        return List.of(ChunkFilterSpecs.crc32(), ChunkFilterSpecs.magicNumber());
    }

    private static List<ChunkFilterSpec> defaultDecodingChunkFilterSpecs() {
        return List.of(ChunkFilterSpecs.magicNumber(),
                ChunkFilterSpecs.crc32());
    }

}
