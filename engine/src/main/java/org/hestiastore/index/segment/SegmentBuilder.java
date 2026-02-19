package org.hestiastore.index.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.Directory;
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
    private static final int DEFAULT_MAX_NUMBER_OF_DELTA_CACHE_FILES = 10;

    private static final int DEFAULT_INDEX_BUFEER_SIZE_IN_BYTES = 1024 * 4;

    private Directory directoryFacade;
    private SegmentId id;
    private TypeDescriptor<K> keyTypeDescriptor;
    private TypeDescriptor<V> valueTypeDescriptor;
    private int maxNumberOfKeysInSegmentWriteCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE;
    private int maxNumberOfKeysInSegmentCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE;
    private int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
    private boolean maxNumberOfKeysInSegmentWriteCacheDuringMaintenanceSet;
    private int maxNumberOfKeysInSegmentChunk = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_CHUNK;
    private int maxNumberOfDeltaCacheFiles = DEFAULT_MAX_NUMBER_OF_DELTA_CACHE_FILES;
    private int bloomFilterNumberOfHashFunctions = SegmentConf.UNSET_BLOOM_FILTER_NUMBER_OF_HASH_FUNCTIONS;
    private int bloomFilterIndexSizeInBytes = SegmentConf.UNSET_BLOOM_FILTER_INDEX_SIZE_IN_BYTES;
    private double bloomFilterProbabilityOfFalsePositive = SegmentConf.UNSET_BLOOM_FILTER_PROBABILITY;
    private int diskIoBufferSize = DEFAULT_INDEX_BUFEER_SIZE_IN_BYTES;
    private final List<ChunkFilter> encodingChunkFilters = new ArrayList<>();
    private final List<ChunkFilter> decodingChunkFilters = new ArrayList<>();
    private Executor maintenanceExecutor;
    private SegmentMaintenancePolicy<K, V> maintenancePolicy;
    private boolean directoryLockingEnabled = true;
    private Runnable compactionRequestListener = () -> {};

    /**
     * Creates a new builder with the required segment directory.
     *
     * @param directoryFacade non-null segment directory facade
     */
    SegmentBuilder(final Directory directoryFacade) {
        withDirectory(directoryFacade);
    }

    /**
     * Sets the directory used to store files for this segment.
     *
     * @param directoryFacade non-null segment directory
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withDirectory(
            final Directory directoryFacade) {
        this.directoryFacade = Vldtn.requireNonNull(directoryFacade,
                "directoryFacade");
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
     * @param id integer id
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withId(final int id) {
        this.id = SegmentId.of(id);
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
     * Sets the maximum number of keys cached across the segment.
     *
     * @param maxNumberOfKeysInSegmentCache value greater than 0
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withMaxNumberOfKeysInSegmentCache(
            final int maxNumberOfKeysInSegmentCache) {
        this.maxNumberOfKeysInSegmentCache = Vldtn.requireGreaterThanZero(
                maxNumberOfKeysInSegmentCache, "maxNumberOfKeysInSegmentCache");
        return this;
    }

    /**
     * Sets the allowed number of write-cache keys during maintenance.
     *
     * @param maxNumberOfKeysInSegmentWriteCacheDuringMaintenance value greater
     *                                                            than 0
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(
            final int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance) {
        this.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance = Vldtn
                .requireGreaterThanZero(
                        maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
                        "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance");
        this.maxNumberOfKeysInSegmentWriteCacheDuringMaintenanceSet = true;
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
     * Sets the maximum number of delta cache files allowed per segment.
     *
     * @param maxNumberOfDeltaCacheFiles max delta cache file count
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withMaxNumberOfDeltaCacheFiles(
            final int maxNumberOfDeltaCacheFiles) {
        this.maxNumberOfDeltaCacheFiles = Vldtn.requireGreaterThanZero(
                maxNumberOfDeltaCacheFiles, "maxNumberOfDeltaCacheFiles");
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
        this.bloomFilterProbabilityOfFalsePositive = probabilityOfFalsePositive == null
                ? SegmentConf.UNSET_BLOOM_FILTER_PROBABILITY
                : probabilityOfFalsePositive;
        return this;
    }


    /**
     * Provide an executor used for maintenance operations (flush/compact).
     *
     * @param maintenanceExecutor executor for maintenance tasks
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withMaintenanceExecutor(
            final Executor maintenanceExecutor) {
        this.maintenanceExecutor = maintenanceExecutor;
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
     * Sets the maintenance policy evaluated after writes.
     *
     * @param maintenancePolicy non-null policy used to decide flush/compact
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withMaintenancePolicy(
            final SegmentMaintenancePolicy<K, V> maintenancePolicy) {
        this.maintenancePolicy = Vldtn.requireNonNull(maintenancePolicy,
                "maintenancePolicy");
        return this;
    }

    /**
     * Enables or disables segment directory locking during build.
     *
     * @param enabled true to enforce directory locking, false to skip it
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withDirectoryLockingEnabled(
            final boolean enabled) {
        this.directoryLockingEnabled = enabled;
        return this;
    }

    /**
     * Sets a callback invoked when compaction is accepted by the segment.
     *
     * @param listener compaction callback
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withCompactionRequestListener(
            final Runnable listener) {
        this.compactionRequestListener = Vldtn.requireNonNull(listener,
                "compactionRequestListener");
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
        final SegmentBuildContext<K, V> context = prepareBuildContext(
                resolveLayout());
        final SegmentDeltaCacheController<K, V> deltaCacheController = new SegmentDeltaCacheController<>(
                context.segmentFiles, context.segmentPropertiesManager,
                context.segmentResources,
                context.segmentConf.getMaxNumberOfKeysInSegmentWriteCache(),
                context.segmentConf.getMaxNumberOfKeysInChunk());
        final SegmentCache<K, V> segmentCache = context.createSegmentCache();
        deltaCacheController.setSegmentCache(segmentCache);
        return new SegmentFullWriterTx<>(context.segmentFiles,
                context.segmentPropertiesManager,
                context.segmentConf.getMaxNumberOfKeysInChunk(),
                context.segmentResources, deltaCacheController);
    }

    /**
     * Build and initialize a {@link Segment} instance. Validates required
     * components, creates defaults where not supplied, and wires dependent
     * parts together.
     *
     * @return build result containing initialized segment or BUSY status when
     *         segment directory lock is already held
     * @throws IllegalArgumentException if required fields are missing or
     *                                  invalid
     */
    public SegmentBuildResult<Segment<K, V>> build() {
        final SegmentDirectoryLayout layout = resolveLayout();
        SegmentDirectoryLocking directoryLocking = null;
        if (directoryLockingEnabled) {
            directoryLocking = new SegmentDirectoryLocking(getDirectoryFacade(),
                    layout);
            if (!directoryLocking.tryLock()) {
                return SegmentBuildResult.busy();
            }
        }
        try {
            final SegmentBuildContext<K, V> context = prepareBuildContext(
                    layout);
            final SegmentSearcher<K, V> segmentSearcher = new SegmentSearcher<>();
            final SegmentDeltaCacheController<K, V> deltaCacheController = new SegmentDeltaCacheController<>(
                    context.segmentFiles, context.segmentPropertiesManager,
                    context.segmentResources,
                    context.segmentConf.getMaxNumberOfKeysInSegmentWriteCache(),
                    context.segmentConf.getMaxNumberOfKeysInChunk());
            final SegmentCache<K, V> segmentCache = context
                    .createSegmentCache();
            deltaCacheController.setSegmentCache(segmentCache);
            final SegmentReadPath<K, V> readPath = new SegmentReadPath<>(
                    context.segmentFiles, context.segmentConf,
                    context.segmentResources, segmentSearcher, segmentCache,
                    context.versionController);
            final SegmentWritePath<K, V> writePath = new SegmentWritePath<>(
                    segmentCache, context.versionController);
            final SegmentMaintenancePath<K, V> maintenancePath = new SegmentMaintenancePath<>(
                    context.segmentFiles, context.segmentConf,
                    context.segmentPropertiesManager, context.segmentResources,
                    deltaCacheController);
            final SegmentCompacter<K, V> compacter = new SegmentCompacter<>(
                    context.versionController);
            final SegmentCore<K, V> core = new SegmentCore<>(
                    context.segmentFiles, context.versionController,
                    context.segmentPropertiesManager, segmentCache, readPath,
                    writePath, maintenancePath);
            final SegmentMaintenancePolicy<K, V> configuredMaintenancePolicy = Vldtn
                    .requireNonNull(maintenancePolicy, "maintenancePolicy");
            return SegmentBuildResult.ok(new SegmentImpl<>(core, compacter,
                    context.maintenanceExecutor, configuredMaintenancePolicy,
                    directoryLocking, compactionRequestListener));
        } catch (final RuntimeException e) {
            if (directoryLocking != null) {
                directoryLocking.unlock();
            }
            throw e;
        }
    }

    private SegmentBuildContext<K, V> prepareBuildContext(
            final SegmentDirectoryLayout layout) {
        return new SegmentBuildContext<>(this, layout);
    }

    private SegmentDirectoryLayout resolveLayout() {
        final SegmentId segmentId = Vldtn.requireNonNull(getId(), "segmentId");
        return new SegmentDirectoryLayout(segmentId);
    }

    Directory getDirectoryFacade() {
        return directoryFacade;
    }

    SegmentId getId() {
        return id;
    }

    TypeDescriptor<K> getKeyTypeDescriptor() {
        return keyTypeDescriptor;
    }

    TypeDescriptor<V> getValueTypeDescriptor() {
        return valueTypeDescriptor;
    }

    int getMaxNumberOfKeysInSegmentWriteCache() {
        return maxNumberOfKeysInSegmentWriteCache;
    }

    int getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance() {
        if (!maxNumberOfKeysInSegmentWriteCacheDuringMaintenanceSet) {
            return Math.max(maxNumberOfKeysInSegmentWriteCache * 2,
                    maxNumberOfKeysInSegmentWriteCache + 1);
        }
        if (maxNumberOfKeysInSegmentWriteCacheDuringMaintenance <= maxNumberOfKeysInSegmentWriteCache) {
            throw new IllegalArgumentException(String.format(
                    "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance must be greater than maxNumberOfKeysInSegmentWriteCache (got %s <= %s)",
                    maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
                    maxNumberOfKeysInSegmentWriteCache));
        }
        return maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
    }

    int getMaxNumberOfKeysInSegmentCache() {
        return maxNumberOfKeysInSegmentCache;
    }

    int getMaxNumberOfKeysInSegmentChunk() {
        return maxNumberOfKeysInSegmentChunk;
    }

    int getMaxNumberOfDeltaCacheFiles() {
        return maxNumberOfDeltaCacheFiles;
    }

    int getBloomFilterNumberOfHashFunctions() {
        return bloomFilterNumberOfHashFunctions;
    }

    int getBloomFilterIndexSizeInBytes() {
        return bloomFilterIndexSizeInBytes;
    }

    double getBloomFilterProbabilityOfFalsePositive() {
        return bloomFilterProbabilityOfFalsePositive;
    }

    int getDiskIoBufferSize() {
        return diskIoBufferSize;
    }

    Executor getMaintenanceExecutor() {
        return maintenanceExecutor;
    }

    List<ChunkFilter> getEncodingChunkFilters() {
        return encodingChunkFilters;
    }

    List<ChunkFilter> getDecodingChunkFilters() {
        return decodingChunkFilters;
    }

}
