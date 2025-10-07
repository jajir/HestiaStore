package org.hestiastore.index.segment;

import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.chunkstore.ChunkFilter;

public final class SegmentBuilder<K, V> {

    private static final int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = 1000
            * 1000 * 10;
    private static final int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE
            * 5;

    private static final int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK = 1000;

    private static final int DEFAULT_INDEX_BUFEER_SIZE_IN_BYTES = 1024 * 4;

    private Directory directory;
    private SegmentId id;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private long maxNumberOfKeysInSegmentCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE;
    private long maxNumberOfKeysInSegmentCacheDuringFlushing = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING;
    private int maxNumberOfKeysInSegmentChunk = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK;
    private Integer bloomFilterNumberOfHashFunctions;
    private Integer bloomFilterIndexSizeInBytes;
    private Double bloomFilterProbabilityOfFalsePositive = null;
    private VersionController versionController;
    private SegmentConf segmentConf;
    private SegmentFiles<K, V> segmentFiles;
    private SegmentDataProvider<K, V> segmentDataProvider;
    private int diskIoBufferSize = DEFAULT_INDEX_BUFEER_SIZE_IN_BYTES;
    private SegmentPropertiesManager segmentPropertiesManager = null;

    SegmentBuilder() {

    }

    public SegmentBuilder<K, V> withDirectory(final Directory directory) {
        this.directory = Vldtn.requireNonNull(directory, "directory");
        return this;
    }

    public SegmentBuilder<K, V> withSegmentConf(final SegmentConf segmentConf) {
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
        return this;
    }

    public SegmentBuilder<K, V> withSegmentFiles(
            final SegmentFiles<K, V> segmentFiles) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        return this;
    }

    public SegmentBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        return this;
    }

    public SegmentBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        return this;
    }

    public SegmentBuilder<K, V> withId(final Integer id) {
        this.id = SegmentId.of(Vldtn.requireNonNull(id, "id"));
        return this;
    }

    public SegmentBuilder<K, V> withId(final SegmentId id) {
        this.id = Vldtn.requireNonNull(id, "id");
        return this;
    }

    public SegmentBuilder<K, V> withMaxNumberOfKeysInSegmentCache(
            final long maxNumberOfKeysInSegmentCache) {
        this.maxNumberOfKeysInSegmentCache = Vldtn.requireNonNull(
                maxNumberOfKeysInSegmentCache, "maxNumberOfKeysInSegmentCache");
        return this;
    }

    public SegmentBuilder<K, V> withMaxNumberOfKeysInSegmentCacheDuringFlushing(
            final long maxNumberOfKeysInSegmentCacheDuringFlushing) {
        this.maxNumberOfKeysInSegmentCacheDuringFlushing = Vldtn.requireNonNull(
                maxNumberOfKeysInSegmentCacheDuringFlushing,
                "maxNumberOfKeysInSegmentCacheDuringFlushing");
        return this;
    }

    public SegmentBuilder<K, V> withMaxNumberOfKeysInSegmentChunk(
            final int maxNumberOfKeysInSegmentChunk) {
        this.maxNumberOfKeysInSegmentChunk = Vldtn.requireNonNull(
                maxNumberOfKeysInSegmentChunk, "maxNumberOfKeysInSegmentChunk");
        return this;
    }

    public SegmentBuilder<K, V> withBloomFilterNumberOfHashFunctions(
            final int bloomFilterNumberOfHashFunctions) {
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        return this;
    }

    public SegmentBuilder<K, V> withBloomFilterIndexSizeInBytes(
            final int bloomFilterIndexSizeInBytes) {
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        return this;
    }

    public SegmentBuilder<K, V> withBloomFilterProbabilityOfFalsePositive(
            final Double probabilityOfFalsePositive) {
        this.bloomFilterProbabilityOfFalsePositive = probabilityOfFalsePositive;
        return this;
    }

    public SegmentBuilder<K, V> withVersionController(
            final VersionController versionController) {
        this.versionController = versionController;
        return this;
    }

    public SegmentBuilder<K, V> withSegmentDataProvider(
            final SegmentDataProvider<K, V> segmentDataProvider) {
        this.segmentDataProvider = segmentDataProvider;
        return this;
    }

    public SegmentBuilder<K, V> withDiskIoBufferSize(
            final int diskIoBufferSize) {
        this.diskIoBufferSize = diskIoBufferSize;
        return this;
    }

    public SegmentBuilder<K, V> withSegmentPropertiesManager(
            final SegmentPropertiesManager segmentPropertiesManager) {
        this.segmentPropertiesManager = segmentPropertiesManager;
        return this;
    }

    public Segment<K, V> build() {
        if (directory == null) {
            throw new IllegalArgumentException("Directory can't be null");
        }
        if (keyTypeDescriptor == null) {
            throw new IllegalArgumentException(
                    "KeyTypeDescriptor can't be null");
        }
        if (valueTypeDescriptor == null) {
            throw new IllegalArgumentException(
                    "ValueTypeDescriptor can't be null");
        }
        if (maxNumberOfKeysInSegmentCache <= 1) {
            throw new IllegalArgumentException(String.format(
                    "maxNumberOfKeysInSegmentCache is '%s' but must be higher than '1'",
                    maxNumberOfKeysInSegmentCache));
        }
        if (maxNumberOfKeysInSegmentCacheDuringFlushing <= maxNumberOfKeysInSegmentCache) {
            throw new IllegalArgumentException(
                    "maxNumberOfKeysInSegmentCacheDuringFlushing must be higher"
                            + " than maxNumberOfKeysInSegmentCache");
        }
        if (versionController == null) {
            versionController = new VersionController();
        }
        if (segmentConf == null) {
            segmentConf = new SegmentConf(maxNumberOfKeysInSegmentCache,
                    maxNumberOfKeysInSegmentCacheDuringFlushing,
                    maxNumberOfKeysInSegmentChunk,
                    bloomFilterNumberOfHashFunctions,
                    bloomFilterIndexSizeInBytes,
                    bloomFilterProbabilityOfFalsePositive, diskIoBufferSize,
                    List.of(), List.of());
        }
        if (segmentFiles == null) {
            final List<ChunkFilter> encodingChunkFilters = Vldtn.requireNonNull(
                    segmentConf.getEncodingChunkFilters(),
                    "encodingChunkFilters");
            final List<ChunkFilter> decodingChunkFilters = Vldtn.requireNonNull(
                    segmentConf.getDecodingChunkFilters(),
                    "decodingChunkFilters");
            segmentFiles = new SegmentFiles<>(directory, id, keyTypeDescriptor,
                    valueTypeDescriptor, segmentConf.getDiskIoBufferSize(),
                    encodingChunkFilters, decodingChunkFilters);
        }
        if (segmentPropertiesManager == null) {
            segmentPropertiesManager = new SegmentPropertiesManager(
                    segmentFiles.getDirectory(), id);
        }
        if (segmentDataProvider == null) {
            final SegmentDataSupplier<K, V> segmentDataSupplier = new SegmentDataSupplier<>(
                    segmentFiles, segmentConf, segmentPropertiesManager);
            final SegmentDataFactory<K, V> segmentDataFactory = new SegmentDataFactoryImpl<>(
                    segmentDataSupplier);
            segmentDataProvider = new SegmentDataProviderSimple<>(
                    segmentDataFactory);
        }
        final SegmentIndexSearcher<K, V> segmentIndexSearcher = new SegmentIndexSearcher<>(
                segmentFiles.getIndexFile(),
                segmentConf.getMaxNumberOfKeysInChunk(),
                segmentFiles.getKeyTypeDescriptor().getComparator());
        final SegmentSearcher<K, V> segmentSearcher = new SegmentSearcher<K, V>(
                segmentFiles.getValueTypeDescriptor(), segmentIndexSearcher,
                segmentDataProvider);
        return new Segment<>(segmentFiles, segmentConf, versionController,
                segmentPropertiesManager, segmentDataProvider, segmentSearcher,
                segmentDataProvider);
    }

}
