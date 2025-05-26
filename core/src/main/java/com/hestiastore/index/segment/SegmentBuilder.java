package com.hestiastore.index.segment;

import java.util.Objects;

import com.hestiastore.index.datatype.TypeDescriptor;
import com.hestiastore.index.directory.Directory;

public final class SegmentBuilder<K, V> {

    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE = 1000
            * 1000 * 10;
    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE
            * 5;

    private final static int DEFAULT_MAX_NUMBER_OF_KEYS_IN_INDEX_PAGE = 1000;

    private final static int DEFAULT_INDEX_BUFEER_SIZE_IN_BYTES = 1024 * 4;

    private Directory directory;
    private SegmentId id;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private long maxNumberOfKeysInSegmentCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE;
    private long maxNumberOfKeysInSegmentCacheDuringFlushing = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CACHE_DURING_FLUSHING;
    private int maxNumberOfKeysInIndexPage = DEFAULT_MAX_NUMBER_OF_KEYS_IN_INDEX_PAGE;
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
        this.directory = Objects.requireNonNull(directory,
                "Directory can't be null");
        return this;
    }

    public SegmentBuilder<K, V> withSegmentConf(final SegmentConf segmentConf) {
        this.segmentConf = Objects.requireNonNull(segmentConf);
        return this;
    }

    public SegmentBuilder<K, V> withSegmentFiles(
            final SegmentFiles<K, V> segmentFiles) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        return this;
    }

    public SegmentBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Objects.requireNonNull(keyTypeDescriptor);
        return this;
    }

    public SegmentBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        return this;
    }

    public SegmentBuilder<K, V> withId(final Integer id) {
        this.id = SegmentId.of(Objects.requireNonNull(id));
        return this;
    }

    public SegmentBuilder<K, V> withId(final SegmentId id) {
        this.id = Objects.requireNonNull(id);
        return this;
    }

    public SegmentBuilder<K, V> withMaxNumberOfKeysInSegmentCache(
            final long maxNumberOfKeysInSegmentCache) {
        this.maxNumberOfKeysInSegmentCache = Objects
                .requireNonNull(maxNumberOfKeysInSegmentCache);
        return this;
    }

    public SegmentBuilder<K, V> withMaxNumberOfKeysInSegmentCacheDuringFlushing(
            final long maxNumberOfKeysInSegmentCacheDuringFlushing) {
        this.maxNumberOfKeysInSegmentCacheDuringFlushing = Objects
                .requireNonNull(maxNumberOfKeysInSegmentCacheDuringFlushing);
        return this;
    }

    public SegmentBuilder<K, V> withMaxNumberOfKeysInIndexPage(
            final int maxNumberOfKeysInIndexPage) {
        this.maxNumberOfKeysInIndexPage = Objects
                .requireNonNull(maxNumberOfKeysInIndexPage);
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
                    maxNumberOfKeysInIndexPage,
                    bloomFilterNumberOfHashFunctions,
                    bloomFilterIndexSizeInBytes,
                    bloomFilterProbabilityOfFalsePositive, diskIoBufferSize);
        }
        if (segmentFiles == null) {
            segmentFiles = new SegmentFiles<>(directory, id, keyTypeDescriptor,
                    valueTypeDescriptor, diskIoBufferSize);
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
        final SegmentIndexSearcherSupplier<K, V> supplier = new SegmentIndexSearcherDefaultSupplier<>(
                segmentFiles, segmentConf);
        final SegmentSearcher<K, V> segmentSearcher = new SegmentSearcher<K, V>(
                segmentFiles.getValueTypeDescriptor(), supplier.get(),
                segmentDataProvider);
        final SegmentManager<K, V> segmentManager = new SegmentManager<>(
                segmentFiles, segmentPropertiesManager, segmentConf,
                segmentDataProvider,
                new SegmentDeltaCacheController<>(segmentFiles,
                        segmentPropertiesManager, segmentDataProvider));

        return new Segment<>(segmentFiles, segmentConf, versionController,
                segmentPropertiesManager, segmentDataProvider, segmentSearcher,
                segmentManager);
    }

}
