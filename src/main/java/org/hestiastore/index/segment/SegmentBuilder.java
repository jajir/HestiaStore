package org.hestiastore.index.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.directory.FileLock;
import org.hestiastore.index.chunkstore.ChunkFilter;
import org.hestiastore.index.segment.SegmentPropertiesManager.SegmentDataState;

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
    private int maxNumberOfKeysInSegmentCache = DEFAULT_MAX_NUMBER_OF_KEYS_IN_SEGMENT_WRITE_CACHE;
    private Integer maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
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
    private Executor maintenanceExecutor;
    private boolean segmentMaintenanceAutoEnabled = true;
    private boolean segmentRootDirectoryEnabled = false;

    /**
     * Creates a new builder with default settings.
     */
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
     * Enables or disables the segment-rooted directory layout.
     *
     * @param enabled true to store files under a per-segment subdirectory
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withSegmentRootDirectoryEnabled(
            final boolean enabled) {
        this.segmentRootDirectoryEnabled = enabled;
        return this;
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
     * @param maxNumberOfKeysInSegmentWriteCacheDuringMaintenance value greater than 0
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(
            final int maxNumberOfKeysInSegmentWriteCacheDuringMaintenance) {
        this.maxNumberOfKeysInSegmentWriteCacheDuringMaintenance = Vldtn
                .requireGreaterThanZero(
                        maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
                        "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance");
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
     * Enables or disables automatic maintenance scheduling after writes.
     *
     * @param enabled true to enable auto maintenance
     * @return this builder for chaining
     */
    public SegmentBuilder<K, V> withSegmentMaintenanceAutoEnabled(
            final boolean enabled) {
        this.segmentMaintenanceAutoEnabled = enabled;
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
                deltaCacheController);
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
    public Segment<K, V> build() {
        prepareBaseComponents();
        final FileLock segmentLock = segmentFiles.acquireLock();
        try {
            final SegmentSearcher<K, V> segmentSearcher = new SegmentSearcher<>();
            final SegmentDeltaCacheController<K, V> deltaCacheController = new SegmentDeltaCacheController<>(
                    segmentFiles, segmentPropertiesManager, segmentResources,
                    segmentConf.getMaxNumberOfKeysInSegmentWriteCache(),
                    segmentConf.getMaxNumberOfKeysInChunk());
            final SegmentCache<K, V> segmentCache = createSegmentCache();
            deltaCacheController.setSegmentCache(segmentCache);
            final SegmentReadPath<K, V> readPath = new SegmentReadPath<>(
                    segmentFiles, segmentConf, segmentResources,
                    segmentSearcher, segmentCache, versionController);
            final SegmentWritePath<K, V> writePath = new SegmentWritePath<>(
                    segmentCache, versionController);
            final SegmentMaintenancePath<K, V> maintenancePath = new SegmentMaintenancePath<>(
                    segmentFiles, segmentConf, segmentPropertiesManager,
                    segmentResources, deltaCacheController);
            final SegmentCompacter<K, V> compacter = new SegmentCompacter<>(
                    versionController);
            final SegmentCore<K, V> core = new SegmentCore<>(segmentFiles,
                    versionController, segmentPropertiesManager, segmentCache,
                    readPath, writePath, maintenancePath);
            final SegmentMaintenancePolicy<K, V> maintenancePolicy = segmentMaintenanceAutoEnabled
                    ? new SegmentMaintenancePolicyThreshold<>(
                            segmentConf.getMaxNumberOfKeysInSegmentCache(),
                            segmentConf
                                    .getMaxNumberOfKeysInSegmentWriteCache())
                    : SegmentMaintenancePolicy.none();
            return new SegmentImpl<>(core, compacter, maintenanceExecutor,
                    maintenancePolicy, segmentLock);
        } catch (final RuntimeException e) {
            if (segmentLock.isLocked()) {
                segmentLock.unlock();
            }
            throw e;
        }
    }

    /**
     * Validates required fields and constructs default dependencies.
     */
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
        final int effectiveMaxKeysDuringMaintenance;
        if (maxNumberOfKeysInSegmentWriteCacheDuringMaintenance == null) {
            effectiveMaxKeysDuringMaintenance = Math.max(
                    maxNumberOfKeysInSegmentWriteCache * 2,
                    maxNumberOfKeysInSegmentWriteCache + 1);
        } else if (maxNumberOfKeysInSegmentWriteCacheDuringMaintenance <= maxNumberOfKeysInSegmentWriteCache) {
            throw new IllegalArgumentException(String.format(
                    "maxNumberOfKeysInSegmentWriteCacheDuringMaintenance must be greater than maxNumberOfKeysInSegmentWriteCache (got %s <= %s)",
                    maxNumberOfKeysInSegmentWriteCacheDuringMaintenance,
                    maxNumberOfKeysInSegmentWriteCache));
        } else {
            effectiveMaxKeysDuringMaintenance = maxNumberOfKeysInSegmentWriteCacheDuringMaintenance;
        }
        if (versionController == null) {
            versionController = new VersionController();
        }
        if (segmentConf == null) {
            Vldtn.requireNotEmpty(encodingChunkFilters, "encodingChunkFilters");
            Vldtn.requireNotEmpty(decodingChunkFilters, "decodingChunkFilters");
            segmentConf = new SegmentConf(
                    (int) maxNumberOfKeysInSegmentWriteCache,
                    effectiveMaxKeysDuringMaintenance, maxNumberOfKeysInSegmentCache,
                    maxNumberOfKeysInSegmentChunk,
                    bloomFilterNumberOfHashFunctions,
                    bloomFilterIndexSizeInBytes,
                    bloomFilterProbabilityOfFalsePositive, diskIoBufferSize,
                    encodingChunkFilters, decodingChunkFilters);
        }
        if (segmentFiles == null) {
            final SegmentId resolvedId = Vldtn.requireNonNull(id,
                    "segmentId");
            final SegmentDirectoryLayout layout = new SegmentDirectoryLayout(
                    resolvedId);
            if (segmentRootDirectoryEnabled) {
                final SegmentDirectoryResolution resolution = resolveDirectoryLayout(
                        directoryFacade, layout, resolvedId);
                segmentFiles = new SegmentFiles<>(resolution.rootDirectory,
                        resolution.activeDirectory,
                        layout,
                        resolution.activeDirectoryName,
                        keyTypeDescriptor, valueTypeDescriptor,
                        segmentConf.getDiskIoBufferSize(),
                        segmentConf.getEncodingChunkFilters(),
                        segmentConf.getDecodingChunkFilters());
            } else {
                segmentFiles = new SegmentFiles<>(directoryFacade, resolvedId,
                        keyTypeDescriptor, valueTypeDescriptor,
                        segmentConf.getDiskIoBufferSize(),
                        segmentConf.getEncodingChunkFilters(),
                        segmentConf.getDecodingChunkFilters());
            }
        }
        if (segmentPropertiesManager == null) {
            segmentPropertiesManager = new SegmentPropertiesManager(
                    segmentFiles.getAsyncDirectory(), id);
            if (segmentFiles.isSegmentRootDirectoryEnabled()) {
                initializeDirectoryMetadata(segmentPropertiesManager,
                        segmentFiles.getActiveDirectoryName());
            }
        }
        if (segmentResources == null) {
            final SegmentDataSupplier<K, V> segmentDataSupplier = new SegmentDataSupplier<>(
                    segmentFiles, segmentConf);
            segmentResources = new SegmentResourcesImpl<>(segmentDataSupplier);
        }
        if (maintenanceExecutor == null) {
            maintenanceExecutor = new DirectExecutor();
        }
    }

    /**
     * Builds the in-memory segment cache using the current configuration.
     *
     * @return initialized segment cache
     */
    private SegmentCache<K, V> createSegmentCache() {
        final SegmentCache<K, V> segmentCache = new SegmentCache<>(
                segmentFiles.getKeyTypeDescriptor().getComparator(),
                segmentFiles.getValueTypeDescriptor(),
                null,
                segmentConf.getMaxNumberOfKeysInSegmentWriteCache(),
                segmentConf.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(),
                segmentConf.getMaxNumberOfKeysInSegmentCache());
        new SegmentDeltaCacheLoader<>(segmentFiles, segmentPropertiesManager)
                .loadInto(segmentCache);
        return segmentCache;
    }

    private SegmentDirectoryResolution resolveDirectoryLayout(
            final AsyncDirectory baseDirectory,
            final SegmentDirectoryLayout layout,
            final SegmentId segmentId) {
        final AsyncDirectory rootDirectory = baseDirectory
                .openSubDirectory(segmentId.getName())
                .toCompletableFuture().join();
        final SegmentDirectoryPointer pointer = new SegmentDirectoryPointer(
                rootDirectory, layout);
        String activeDirectoryName = pointer.readActiveDirectory();
        if (activeDirectoryName == null) {
            activeDirectoryName = hasLegacyFiles(rootDirectory, layout)
                    ? SegmentDirectoryLayout.ROOT_DIRECTORY_NAME
                    : SegmentDirectoryLayout.getVersionDirectoryName(1);
            pointer.writeActiveDirectory(activeDirectoryName);
        }
        final AsyncDirectory activeDirectory = openActiveDirectory(rootDirectory,
                activeDirectoryName);
        final SegmentDirectoryResolution resolved = new SegmentDirectoryResolution(
                rootDirectory, activeDirectory, activeDirectoryName);
        return recoverPreparedDirectory(resolved, layout, segmentId, pointer);
    }

    private SegmentDirectoryResolution recoverPreparedDirectory(
            final SegmentDirectoryResolution resolved,
            final SegmentDirectoryLayout layout,
            final SegmentId segmentId,
            final SegmentDirectoryPointer pointer) {
        final long activeVersion = resolveVersion(resolved.activeDirectoryName);
        if (activeVersion < 0) {
            return resolved;
        }
        final String preparedDirectoryName = SegmentDirectoryLayout
                .getVersionDirectoryName(activeVersion + 1);
        final AsyncDirectory preparedDirectory = resolved.rootDirectory
                .openSubDirectory(preparedDirectoryName)
                .toCompletableFuture().join();
        final boolean preparedPropertiesExists = preparedDirectory
                .isFileExistsAsync(layout.getPropertiesFileName())
                .toCompletableFuture().join();
        if (!preparedPropertiesExists) {
            return resolved;
        }
        final SegmentPropertiesManager preparedProperties = new SegmentPropertiesManager(
                preparedDirectory, segmentId);
        final SegmentDataState preparedState = preparedProperties.getState();
        if (preparedState != SegmentDataState.PREPARED
                && preparedState != SegmentDataState.ACTIVE) {
            return resolved;
        }
        final boolean preparedIndexExists = preparedDirectory
                .isFileExistsAsync(layout.getIndexFileName())
                .toCompletableFuture().join();
        if (!preparedIndexExists) {
            return resolved;
        }
        pointer.writeActiveDirectory(preparedDirectoryName);
        preparedProperties.setState(SegmentDataState.ACTIVE);
        return new SegmentDirectoryResolution(resolved.rootDirectory,
                preparedDirectory, preparedDirectoryName);
    }

    private AsyncDirectory openActiveDirectory(
            final AsyncDirectory rootDirectory,
            final String activeDirectoryName) {
        if (SegmentDirectoryLayout.ROOT_DIRECTORY_NAME
                .equals(activeDirectoryName)) {
            return rootDirectory;
        }
        return rootDirectory.openSubDirectory(activeDirectoryName)
                .toCompletableFuture().join();
    }

    private boolean hasLegacyFiles(final AsyncDirectory rootDirectory,
            final SegmentDirectoryLayout layout) {
        return rootDirectory.isFileExistsAsync(layout.getPropertiesFileName())
                .toCompletableFuture().join();
    }

    private long resolveVersion(final String directoryName) {
        if (SegmentDirectoryLayout.ROOT_DIRECTORY_NAME.equals(directoryName)) {
            return 0;
        }
        return SegmentDirectoryLayout.parseVersionDirectoryName(directoryName);
    }

    private void initializeDirectoryMetadata(
            final SegmentPropertiesManager propertiesManager,
            final String activeDirectoryName) {
        final long expectedVersion = resolveVersion(activeDirectoryName);
        if (expectedVersion >= 0
                && propertiesManager.getVersion() != expectedVersion) {
            propertiesManager.setVersion(expectedVersion);
        }
        propertiesManager.setState(SegmentDataState.ACTIVE);
    }

    private static final class SegmentDirectoryResolution {
        private final AsyncDirectory rootDirectory;
        private final AsyncDirectory activeDirectory;
        private final String activeDirectoryName;

        private SegmentDirectoryResolution(final AsyncDirectory rootDirectory,
                final AsyncDirectory activeDirectory,
                final String activeDirectoryName) {
            this.rootDirectory = rootDirectory;
            this.activeDirectory = activeDirectory;
            this.activeDirectoryName = activeDirectoryName;
        }
    }

}
