package org.hestiastore.index.segment;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.chunkstore.ChunkFilter;

/**
 * Builder for {@link Segment}.
 *
 * @param <K> the type of keys maintained by the segment
 * @param <V> the type of mapped values
 */
public final class SegmentBuilder<K, V> {

    private static final int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE = 1000
            * 1000 * 5;
    private static final int DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK = 1000;

    private static final int DEFAULT_INDEX_BUFEER_SIZE_IN_BYTES = 1024 * 4;

    private AsyncDirectory directoryFacade;
    private SegmentId id;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private int maxNumberOfKeysInSegmentWriteCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE;
    private int maxNumberOfKeysInSegmentChunk = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK;
    private Integer bloomFilterNumberOfHashFunctions;
    private Integer bloomFilterIndexSizeInBytes;
    private Double bloomFilterProbabilityOfFalsePositive = null;
    private VersionController versionController;
    private SegmentConf segmentConf;
    private SegmentFiles<K, V> segmentFiles;
    private SegmentResources<K, V> segmentResources;
    private int diskIoBufferSize = DEFAULT_INDEX_BUFEER_SIZE_IN_BYTES;
    private SegmentPropertiesManager segmentPropertiesManager = null;
    private final List<ChunkFilter> encodingChunkFilters = new ArrayList<>();
    private final List<ChunkFilter> decodingChunkFilters = new ArrayList<>();

    SegmentBuilder() {

    }

    /**
     * Set the base {@link AsyncDirectory} used to store files for this segment.
     *
     * @param directoryFacade non-null directory facade
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withDirectory(
            final AsyncDirectory directoryFacade) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
        return this;
    }

    /**
     * Backwards-compatible alias for {@link #withDirectory(AsyncDirectory)}.
     *
     * @param directoryFacade non-null directory facade
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withAsyncDirectory(
            final AsyncDirectory directoryFacade) {
        return withDirectory(directoryFacade);
    }

    /**
     * Provide the {@link SegmentConf} to use. When not provided, it will be
     * created from values configured on this builder.
     *
     * @param segmentConf non-null segment configuration
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withSegmentConf(final SegmentConf segmentConf) {
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
        return this;
    }

    /**
     * Provide pre-initialized {@link SegmentFiles}. When not provided, they
     * will be created during {@link #build()} using directory, id and type
     * descriptors.
     *
     * @param segmentFiles non-null segment files wrapper
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withSegmentFiles(
            final SegmentFiles<K, V> segmentFiles) {
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        return this;
    }

    /**
     * Set key {@link TypeDescriptor} used for serialization and comparison.
     *
     * @param keyTypeDescriptor non-null type descriptor for keys
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withKeyTypeDescriptor(
            final TypeDescriptor<K> keyTypeDescriptor) {
        this.keyTypeDescriptor = Vldtn.requireNonNull(keyTypeDescriptor,
                "keyTypeDescriptor");
        return this;
    }

    /**
     * Set value {@link TypeDescriptor} used for serialization.
     *
     * @param valueTypeDescriptor non-null type descriptor for values
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withValueTypeDescriptor(
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.valueTypeDescriptor = Vldtn.requireNonNull(valueTypeDescriptor,
                "valueTypeDescriptor");
        return this;
    }

    /**
     * Set segment identifier by integer value.
     *
     * @param id non-null integer id
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withId(final Integer id) {
        this.id = SegmentId.of(Vldtn.requireNonNull(id, "id"));
        return this;
    }

    /**
     * Set segment identifier.
     *
     * @param id non-null segment id
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withId(final SegmentId id) {
        this.id = Vldtn.requireNonNull(id, "id");
        return this;
    }

    /**
     * Set maximum number of keys cached in memory while collecting writes
     * before flushing to a delta cache file.
     *
     * @param maxNumberOfKeysInSegmentWriteCache value greater than 0
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withMaxNumberOfKeysInSegmentWriteCache(
            final int maxNumberOfKeysInSegmentWriteCache) {
        this.maxNumberOfKeysInSegmentWriteCache = Vldtn.requireGreaterThanZero(
                maxNumberOfKeysInSegmentWriteCache,
                "maxNumberOfKeysInSegmentWriteCache");
        return this;
    }

    /**
     * Set maximum number of keys stored within a single segment chunk on disk.
     *
     * @param maxNumberOfKeysInSegmentChunk positive chunk size
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withMaxNumberOfKeysInSegmentChunk(
            final int maxNumberOfKeysInSegmentChunk) {
        this.maxNumberOfKeysInSegmentChunk = Vldtn.requireGreaterThanZero(
                maxNumberOfKeysInSegmentChunk, "maxNumberOfKeysInSegmentChunk");
        return this;
    }

    /**
     * Configure Bloom filter hash functions count used by the index.
     *
     * @param bloomFilterNumberOfHashFunctions number of hash functions
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withBloomFilterNumberOfHashFunctions(
            final int bloomFilterNumberOfHashFunctions) {
        this.bloomFilterNumberOfHashFunctions = bloomFilterNumberOfHashFunctions;
        return this;
    }

    /**
     * Configure Bloom filter index size in bytes.
     *
     * @param bloomFilterIndexSizeInBytes target size in bytes
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withBloomFilterIndexSizeInBytes(
            final int bloomFilterIndexSizeInBytes) {
        this.bloomFilterIndexSizeInBytes = bloomFilterIndexSizeInBytes;
        return this;
    }

    /**
     * Configure desired probability of false positive for Bloom filter.
     *
     * @param probabilityOfFalsePositive probability in range (0, 1]
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withBloomFilterProbabilityOfFalsePositive(
            final Double probabilityOfFalsePositive) {
        this.bloomFilterProbabilityOfFalsePositive = probabilityOfFalsePositive;
        return this;
    }

    /**
     * Provide a {@link VersionController}. When not provided, a new controller
     * is created during {@link #build()}.
     *
     * @param versionController controller instance
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withVersionController(
            final VersionController versionController) {
        this.versionController = versionController;
        return this;
    }

    /**
     * Provide a {@link SegmentResources}. When not provided it will be created
     * during {@link #build()}.
     *
     * @param segmentDataProvider provider instance
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withSegmentResources(
            final SegmentResources<K, V> segmentDataProvider) {
        this.segmentResources = segmentDataProvider;
        return this;
    }

    /**
     * Set disk I/O buffer size used for index/data file operations.
     *
     * @param diskIoBufferSize buffer size in bytes
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withDiskIoBufferSize(
            final int diskIoBufferSize) {
        this.diskIoBufferSize = Vldtn.requireIoBufferSize(diskIoBufferSize);
        return this;
    }

    /**
     * Provide a {@link SegmentPropertiesManager}. When not provided, a default
     * one will be created during {@link #build()}.
     *
     * @param segmentPropertiesManager properties manager instance
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withSegmentPropertiesManager(
            final SegmentPropertiesManager segmentPropertiesManager) {
        this.segmentPropertiesManager = segmentPropertiesManager;
        return this;
    }

    /**
     * Set the list of chunk filters applied during encoding (write path).
     *
     * @param filters non-empty list of encoding filters
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withEncodingChunkFilters(
            final List<ChunkFilter> filters) {
        final List<ChunkFilter> validated = Vldtn.requireNotEmpty(filters,
                "encodingChunkFilters");
        encodingChunkFilters.clear();
        encodingChunkFilters.addAll(List.copyOf(validated));
        return this;
    }

    /**
     * Set the list of chunk filters applied during decoding (read path).
     *
     * @param filters non-empty list of decoding filters
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withDecodingChunkFilters(
            final List<ChunkFilter> filters) {
        final List<ChunkFilter> validated = Vldtn.requireNotEmpty(filters,
                "decodingChunkFilters");
        decodingChunkFilters.clear();
        decodingChunkFilters.addAll(List.copyOf(validated));
        return this;
    }

    /**
     * Opens a full writer transaction that builds the segment from a sorted
     * stream of entries. Entries must be unique, sorted by key in ascending
     * order, and must not contain tombstones. The returned transaction writes
     * directly to the main index and scarce index files.
     *
     * @return transaction for streaming the segment contents
     */
    public SegmentFullWriterTx<K, V> openWriterTx() {
        prepareBaseComponents();
        final SegmentDeltaCacheController<K, V> deltaCacheController = new SegmentDeltaCacheController<>(
                segmentFiles, segmentPropertiesManager, segmentResources,
                segmentConf.getMaxNumberOfKeysInSegmentWriteCache(),
                segmentConf.getMaxNumberOfKeysInChunk());
        final SegmentCache<K, V> segmentCache = createSegmentCache();
        deltaCacheController.setSegmentCache(segmentCache);
        return new SegmentFullWriterTx<>(segmentFiles, segmentPropertiesManager,
                segmentConf.getMaxNumberOfKeysInChunk(), segmentResources,
                deltaCacheController, segmentCache);
    }

    /**
     * Build and initialize a {@link Segment} instance. Validates required
     * components, creates defaults where not supplied, and wires dependent
     * parts together.
     *
     * @return initialized segment instance
     * @throws IllegalArgumentException if required fields are missing or
     *                                  invalid
     */
    public SegmentImpl<K, V> build() {
        prepareBaseComponents();
        final SegmentSearcher<K, V> segmentSearcher = new SegmentSearcher<K, V>(
                segmentFiles.getValueTypeDescriptor());
        final SegmentDeltaCacheController<K, V> deltaCacheController = new SegmentDeltaCacheController<>(
                segmentFiles, segmentPropertiesManager, segmentResources,
                segmentConf.getMaxNumberOfKeysInSegmentWriteCache(),
                segmentConf.getMaxNumberOfKeysInChunk());
        final SegmentCompacter<K, V> compacter = new SegmentCompacter<>(
                versionController);
        return new SegmentImpl<>(segmentFiles, segmentConf, versionController,
                segmentPropertiesManager, segmentResources,
                deltaCacheController, segmentSearcher, compacter);
    }

    private void prepareBaseComponents() {
        if (directoryFacade == null) {
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
        if (maxNumberOfKeysInSegmentWriteCache <= 0) {
            throw new IllegalArgumentException(String.format(
                    "maxNumberOfKeysInSegmentWriteCache is '%s' but must be higher than '0'",
                    maxNumberOfKeysInSegmentWriteCache));
        }
        if (versionController == null) {
            versionController = new VersionController();
        }
        if (segmentConf == null) {
            Vldtn.requireNotEmpty(encodingChunkFilters, "encodingChunkFilters");
            Vldtn.requireNotEmpty(decodingChunkFilters, "decodingChunkFilters");
            segmentConf = new SegmentConf(
                    (int) maxNumberOfKeysInSegmentWriteCache,
                    maxNumberOfKeysInSegmentChunk,
                    bloomFilterNumberOfHashFunctions,
                    bloomFilterIndexSizeInBytes,
                    bloomFilterProbabilityOfFalsePositive, diskIoBufferSize,
                    encodingChunkFilters, decodingChunkFilters);
        }
        if (segmentFiles == null) {
            segmentFiles = new SegmentFiles<>(directoryFacade, id,
                    keyTypeDescriptor, valueTypeDescriptor,
                    segmentConf.getDiskIoBufferSize(),
                    segmentConf.getEncodingChunkFilters(),
                    segmentConf.getDecodingChunkFilters());
        }
        if (segmentPropertiesManager == null) {
            segmentPropertiesManager = new SegmentPropertiesManager(
                    segmentFiles.getAsyncDirectory(), id);
        }
        if (segmentResources == null) {
            final SegmentDataSupplier<K, V> segmentDataSupplier = new SegmentDataSupplier<>(
                    segmentFiles, segmentConf, segmentPropertiesManager);
            segmentResources = new SegmentResourcesImpl<>(segmentDataSupplier);
        }
    }

    private SegmentCache<K, V> createSegmentCache() {
        final SegmentDeltaCache<K, V> deltaCache = segmentResources
                .getSegmentDeltaCache();
        return new SegmentCache<>(
                segmentFiles.getKeyTypeDescriptor().getComparator(),
                segmentFiles.getValueTypeDescriptor(),
                deltaCache.getAsSortedList());
    }

}
