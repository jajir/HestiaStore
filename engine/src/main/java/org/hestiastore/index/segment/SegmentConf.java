package org.hestiastore.index.segment;

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import org.hestiastore.index.chunkstore.ChunkFilter;

/**
 * Immutable configuration values for a segment instance.
 */
@SuppressWarnings({ "java:S107", "java:S1133" })
public class SegmentConf {

    /**
     * Sentinel value for unset Bloom filter hash function count.
     */
    public static final int UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS = -1;

    /**
     * Sentinel value for unset Bloom filter index size.
     */
    public static final int UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES = -1;

    /**
     * Sentinel value for unset Bloom filter false positive probability.
     */
    public static final double UNSET_BLOOM_FILTER_PROBABILITY = -1D;

    private final int maxNumberOfKeysInSegmentWriteCache;
    private final int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
    private final int maxNumberOfKeysInSegmentCache;
    private final int maxNumberOfKeysInChunk;
    private final int maxNumberOfDeltaCacheFiles;
    private final int bloomFilterNumberOfHashFunctions;
    private final int bloomFilterIndexSizeInBytes;
    private final double bloomFilterProbabilityOfFalsePositive;
    private final int diskIoBufferSize;
    private final List<Supplier<? extends ChunkFilter>> encodingChunkFilters;
    private final List<Supplier<? extends ChunkFilter>> decodingChunkFilters;

    private SegmentConf(final Builder builder) {
        maxNumberOfKeysInSegmentWriteCache = requireSet(
                builder.maxNumberOfKeysInSegmentWriteCache,
                "maxNumberOfKeysInSegmentWriteCache");
        maxNumberOfKeysInSegmentWriteCacheDuringMaintenance = requireSet(
                builder.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
                "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance");
        maxNumberOfKeysInSegmentCache = requireSet(
                builder.maxNumberOfKeysInSegmentCache,
                "maxNumberOfKeysInSegmentCache");
        maxNumberOfKeysInChunk = requireSet(builder.maxNumberOfKeysInChunk,
                "maxNumberOfKeysInChunk");
        maxNumberOfDeltaCacheFiles = requireSet(
                builder.maxNumberOfDeltaCacheFiles,
                "maxNumberOfDeltaCacheFiles");
        bloomFilterNumberOfHashFunctions = builder.bloomFilterNumberOfHashFunctions;
        bloomFilterIndexSizeInBytes = builder.bloomFilterIndexSizeInBytes;
        bloomFilterProbabilityOfFalsePositive = builder.bloomFilterProbabilityOfFalsePositive;
        diskIoBufferSize = requireSet(builder.diskIoBufferSize,
                "diskIoBufferSize");
        encodingChunkFilters = List.copyOf(Objects.requireNonNull(
                builder.encodingChunkFilters, "encodingChunkFilters"));
        decodingChunkFilters = List.copyOf(Objects.requireNonNull(
                builder.decodingChunkFilters, "decodingChunkFilters"));
    }

    /**
     * Creates a fluent builder for immutable {@link SegmentConf} instances.
     *
     * @return new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a fluent builder pre-populated from an existing configuration.
     *
     * @param segmentConf source configuration
     * @return new builder initialized from source values
     */
    public static Builder builder(final SegmentConf segmentConf) {
        return new Builder(segmentConf);
    }

    /**
     * Creates a configuration with explicit values for all fields.
     *
     * @param maxNumberOfKeysInSegmentWriteCache max write-cache size
     * @param maxNumberOfKeysInSegmentWriteCacheDuringMaintenance write-cache size
     *        allowed during maintenance
     * @param maxNumberOfKeysInSegmentCache max segment cache size
     * @param maxNumberOfKeysInChunk max number of keys in a chunk
     * @param maxNumberOfDeltaCacheFiles max delta cache file count
     * @param bloomFilterNumberOfHashFunctions Bloom filter hash count or
     *        {@link #UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS}
     * @param bloomFilterIndexSizeInBytes Bloom filter index size in bytes or
     *        {@link #UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES}
     * @param bloomFilterProbabilityOfFalsePositive Bloom filter false positive
     *        probability or {@link #UNSET_BLOOM_FILTER_PROBABILITY}
     * @param diskIoBufferSize disk I/O buffer size in bytes
     * @param encodingChunkFilters chunk filters applied during encoding
     * @param decodingChunkFilters chunk filters applied during decoding
     * @deprecated use {@link #builder()} for clearer named configuration
     */
    @Deprecated(since = "0.0.7")
    public SegmentConf(final int maxNumberOfKeysInSegmentWriteCache,
            final int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
            final int maxNumberOfKeysInSegmentCache,
            final int maxNumberOfKeysInChunk,
            final int maxNumberOfDeltaCacheFiles,
            final int bloomFilterNumberOfHashFunctions,
            final int bloomFilterIndexSizeInBytes,
            final double bloomFilterProbabilityOfFalsePositive,
            final int diskIoBufferSize,
            final List<ChunkFilter> encodingChunkFilters,
            final List<ChunkFilter> decodingChunkFilters) {
        this(SegmentConf.builder()
                .withMaxNumberOfKeysInSegmentWriteCache(
                        maxNumberOfKeysInSegmentWriteCache)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(
                        maxNumberOfKeysInSegmentWriteCacheDuringMaintenance)
                .withMaxNumberOfKeysInSegmentCache(maxNumberOfKeysInSegmentCache)
                .withMaxNumberOfKeysInChunk(maxNumberOfKeysInChunk)
                .withMaxNumberOfDeltaCacheFiles(maxNumberOfDeltaCacheFiles)
                .withBloomFilterNumberOfHashFunctions(
                        bloomFilterNumberOfHashFunctions)
                .withBloomFilterIndexSizeInBytes(bloomFilterIndexSizeInBytes)
                .withBloomFilterProbabilityOfFalsePositive(
                        bloomFilterProbabilityOfFalsePositive)
                .withDiskIoBufferSize(diskIoBufferSize)
                .withEncodingChunkFilters(encodingChunkFilters)
                .withDecodingChunkFilters(decodingChunkFilters));
    }

    /**
     * Creates a copy of an existing configuration.
     *
     * @param segmentConf source configuration
     */
    public SegmentConf(final SegmentConf segmentConf) {
        this(SegmentConf.builder(segmentConf));
    }

    private static int requireSet(final Integer value,
            final String propertyName) {
        return Objects
                .requireNonNull(value, "Property '" + propertyName + "' must be set.");
    }

    /**
     * Returns the write-cache key limit for a segment.
     *
     * @return max number of keys in the write cache
     */
    int getMaxNumberOfKeysInSegmentWriteCache() {
        return maxNumberOfKeysInSegmentWriteCache;
    }

    /**
     * Returns the maximum write-cache size allowed during maintenance.
     *
     * @return max number of keys in write cache during maintenance
     */
    int getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance() {
        return maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
    }

    /**
     * Returns the maximum cached key count for the segment.
     *
     * @return max number of keys in segment cache
     */
    int getMaxNumberOfKeysInSegmentCache() {
        return maxNumberOfKeysInSegmentCache;
    }

    /**
     * Returns the maximum number of keys per chunk.
     *
     * @return max keys in a chunk
     */
    int getMaxNumberOfKeysInChunk() {
        return maxNumberOfKeysInChunk;
    }

    /**
     * Returns the maximum number of delta cache files allowed per segment.
     *
     * @return max delta cache file count
     */
    int getMaxNumberOfDeltaCacheFiles() {
        return maxNumberOfDeltaCacheFiles;
    }

    /**
     * Returns the Bloom filter hash function count.
     *
     * @return number of hash functions or
     *         {@link #UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS}
     */
    int getBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions;
    }

    /**
     * Returns the Bloom filter index size in bytes.
     *
     * @return index size in bytes or
     *         {@link #UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES}
     */
    int getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    /**
     * Returns the configured Bloom filter false positive probability.
     *
     * @return false positive probability or
     *         {@link #UNSET_BLOOM_FILTER_PROBABILITY}
     */
    public double getBloomFilterProbabilityOfFalsePositive() {
        return bloomFilterProbabilityOfFalsePositive;
    }

    /**
     * Returns the disk I/O buffer size in bytes.
     *
     * @return buffer size in bytes
     */
    public int getDiskIoBufferSize() {
        return diskIoBufferSize;
    }

    /**
     * Returns whether Bloom filter hash function count was configured.
     *
     * @return true when hash count is set
     */
    boolean hasBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions != UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS;
    }

    /**
     * Returns whether Bloom filter index size was configured.
     *
     * @return true when index size is set
     */
    boolean hasBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes != UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES;
    }

    /**
     * Returns whether Bloom filter false positive probability was configured.
     *
     * @return true when probability is set
     */
    boolean hasBloomFilterProbabilityOfFalsePositive() {
        return Double.compare(bloomFilterProbabilityOfFalsePositive,
                UNSET_BLOOM_FILTER_PROBABILITY) != 0;
    }

    /**
     * Returns the encoding chunk filter chain.
     *
     * @return encoding filters
     */
    public List<ChunkFilter> getEncodingChunkFilters() {
        return encodingChunkFilters.stream()
                .map(supplier -> (ChunkFilter) supplier.get()).toList();
    }

    /**
     * Returns the decoding chunk filter chain.
     *
     * @return decoding filters
     */
    public List<ChunkFilter> getDecodingChunkFilters() {
        return decodingChunkFilters.stream()
                .map(supplier -> (ChunkFilter) supplier.get()).toList();
    }

    List<Supplier<? extends ChunkFilter>> getEncodingChunkFilterSuppliers() {
        return encodingChunkFilters;
    }

    List<Supplier<? extends ChunkFilter>> getDecodingChunkFilterSuppliers() {
        return decodingChunkFilters;
    }

    /**
     * Fluent builder for {@link SegmentConf}.
     */
    public static final class Builder {

        private Integer maxNumberOfKeysInSegmentWriteCache;
        private Integer maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
        private Integer maxNumberOfKeysInSegmentCache;
        private Integer maxNumberOfKeysInChunk;
        private Integer maxNumberOfDeltaCacheFiles;
        private int bloomFilterNumberOfHashFunctions = UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS;
        private int bloomFilterIndexSizeInBytes = UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES;
        private double bloomFilterProbabilityOfFalsePositive = UNSET_BLOOM_FILTER_PROBABILITY;
        private Integer diskIoBufferSize;
        private List<Supplier<? extends ChunkFilter>> encodingChunkFilters;
        private List<Supplier<? extends ChunkFilter>> decodingChunkFilters;

        private Builder() {
        }

        private Builder(final SegmentConf segmentConf) {
            Objects.requireNonNull(segmentConf, "segmentConf");
            maxNumberOfKeysInSegmentWriteCache = segmentConf.maxNumberOfKeysInSegmentWriteCache;
            maxNumberOfKeysInSegmentWriteCacheDuringMaintenance = segmentConf.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
            maxNumberOfKeysInSegmentCache = segmentConf.maxNumberOfKeysInSegmentCache;
            maxNumberOfKeysInChunk = segmentConf.maxNumberOfKeysInChunk;
            maxNumberOfDeltaCacheFiles = segmentConf.maxNumberOfDeltaCacheFiles;
            bloomFilterNumberOfHashFunctions = segmentConf.bloomFilterNumberOfHashFunctions;
            bloomFilterIndexSizeInBytes = segmentConf.bloomFilterIndexSizeInBytes;
            bloomFilterProbabilityOfFalsePositive = segmentConf.bloomFilterProbabilityOfFalsePositive;
            diskIoBufferSize = segmentConf.diskIoBufferSize;
            encodingChunkFilters = segmentConf.encodingChunkFilters;
            decodingChunkFilters = segmentConf.decodingChunkFilters;
        }

        public Builder withMaxNumberOfKeysInSegmentWriteCache(
                final int value) {
            maxNumberOfKeysInSegmentWriteCache = value;
            return this;
        }

        public Builder withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(
                final int value) {
            maxNumberOfKeysInSegmentWriteCacheDuringMaintenance = value;
            return this;
        }

        public Builder withMaxNumberOfKeysInSegmentCache(final int value) {
            maxNumberOfKeysInSegmentCache = value;
            return this;
        }

        public Builder withMaxNumberOfKeysInChunk(final int value) {
            maxNumberOfKeysInChunk = value;
            return this;
        }

        public Builder withMaxNumberOfDeltaCacheFiles(final int value) {
            maxNumberOfDeltaCacheFiles = value;
            return this;
        }

        public Builder withBloomFilterNumberOfHashFunctions(
                final int value) {
            bloomFilterNumberOfHashFunctions = value;
            return this;
        }

        public Builder withBloomFilterIndexSizeInBytes(final int value) {
            bloomFilterIndexSizeInBytes = value;
            return this;
        }

        public Builder withBloomFilterProbabilityOfFalsePositive(
                final double value) {
            bloomFilterProbabilityOfFalsePositive = value;
            return this;
        }

        /**
         * Sets disk I/O buffer size in bytes.
         *
         * @param value buffer size in bytes
         * @return this builder
         */
        public Builder withDiskIoBufferSize(final int value) {
            diskIoBufferSize = value;
            return this;
        }

        /**
         * Sets fixed encoding filters for the segment configuration.
         *
         * @param filters ordered encoding filters
         * @return this builder
         */
        public Builder withEncodingChunkFilters(
                final List<ChunkFilter> filters) {
            encodingChunkFilters = List
                    .copyOf(Objects.requireNonNull(filters, "filters").stream()
                            .map(filter -> (Supplier<? extends ChunkFilter>) () -> filter)
                            .toList());
            return this;
        }

        /**
         * Sets fixed decoding filters for the segment configuration.
         *
         * @param filters ordered decoding filters
         * @return this builder
         */
        public Builder withDecodingChunkFilters(
                final List<ChunkFilter> filters) {
            decodingChunkFilters = List
                    .copyOf(Objects.requireNonNull(filters, "filters").stream()
                            .map(filter -> (Supplier<? extends ChunkFilter>) () -> filter)
                            .toList());
            return this;
        }

        /**
         * Sets encoding filters as runtime suppliers.
         *
         * @param filters ordered encoding filter suppliers
         * @return this builder
         */
        Builder withEncodingChunkFilterSuppliers(
                final List<Supplier<? extends ChunkFilter>> filters) {
            encodingChunkFilters = List.copyOf(
                    Objects.requireNonNull(filters, "filters"));
            return this;
        }

        /**
         * Sets decoding filters as runtime suppliers.
         *
         * @param filters ordered decoding filter suppliers
         * @return this builder
         */
        Builder withDecodingChunkFilterSuppliers(
                final List<Supplier<? extends ChunkFilter>> filters) {
            decodingChunkFilters = List.copyOf(
                    Objects.requireNonNull(filters, "filters"));
            return this;
        }

        /**
         * Builds an immutable segment configuration snapshot.
         *
         * @return immutable configuration
         */
        public SegmentConf build() {
            return new SegmentConf(this);
        }
    }
}
