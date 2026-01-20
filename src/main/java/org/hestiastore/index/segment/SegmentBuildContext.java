package org.hestiastore.index.segment;

import java.util.concurrent.Executor;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.directory.async.AsyncDirectory;

/**
 * Assembles segment components from a configured {@link SegmentBuilder} and
 * exposes them for construction flows.
 *
 * @param <K> the type of keys maintained by the segment
 * @param <V> the type of mapped values
 */
final class SegmentBuildContext<K, V> {

    final VersionController versionController;
    final SegmentConf segmentConf;
    final SegmentFiles<K, V> segmentFiles;
    final SegmentResources<K, V> segmentResources;
    final SegmentPropertiesManager segmentPropertiesManager;
    final Executor maintenanceExecutor;

    /**
     * Builds a context snapshot, validating required fields and creating
     * default components as needed.
     *
     * @param builder configured builder
     */
    SegmentBuildContext(final SegmentBuilder<K, V> builder) {
        final AsyncDirectory directoryFacade = Vldtn.requireNonNull(
                builder.getDirectoryFacade(), "directoryFacade");
        final TypeDescriptor<K> keyTypeDescriptor = Vldtn.requireNonNull(
                builder.getKeyTypeDescriptor(), "keyTypeDescriptor");
        final TypeDescriptor<V> valueTypeDescriptor = Vldtn.requireNonNull(
                builder.getValueTypeDescriptor(), "valueTypeDescriptor");
        final int maxNumberOfKeysInSegmentWriteCache = Vldtn
                .requireGreaterThanZero(
                        builder.getMaxNumberOfKeysInSegmentWriteCache(),
                        "maxNumberOfKeysInSegmentWriteCache");
        versionController = new VersionController();

        Vldtn.requireNotEmpty(builder.getEncodingChunkFilters(),
                "encodingChunkFilters");
        Vldtn.requireNotEmpty(builder.getDecodingChunkFilters(),
                "decodingChunkFilters");
        segmentConf = new SegmentConf(maxNumberOfKeysInSegmentWriteCache,
                builder.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(),
                builder.getMaxNumberOfKeysInSegmentCache(),
                builder.getMaxNumberOfKeysInSegmentChunk(),
                builder.getBloomFilterNumberOfHashFunctions(),
                builder.getBloomFilterIndexSizeInBytes(),
                builder.getBloomFilterProbabilityOfFalsePositive(),
                builder.getDiskIoBufferSize(),
                builder.getEncodingChunkFilters(),
                builder.getDecodingChunkFilters());

        final SegmentId resolvedId = Vldtn.requireNonNull(builder.getId(),
                "segmentId");
        final SegmentDirectoryLayout layout = new SegmentDirectoryLayout(
                resolvedId);
        final SegmentPropertiesManager propertiesManager = new SegmentPropertiesManager(
                directoryFacade, resolvedId);
        final long activeVersion = resolveActiveVersion(directoryFacade, layout,
                propertiesManager);
        segmentFiles = new SegmentFiles<>(directoryFacade, layout,
                activeVersion, keyTypeDescriptor, valueTypeDescriptor,
                segmentConf.getDiskIoBufferSize(),
                segmentConf.getEncodingChunkFilters(),
                segmentConf.getDecodingChunkFilters());

        segmentPropertiesManager = propertiesManager;
        initializeDirectoryMetadata(segmentPropertiesManager, activeVersion);

        final SegmentDataSupplier<K, V> segmentDataSupplier = new SegmentDataSupplier<>(
                segmentFiles, segmentConf);
        segmentResources = new SegmentResourcesImpl<>(segmentDataSupplier);

        maintenanceExecutor = builder.getMaintenanceExecutor() == null
                ? new DirectExecutor()
                : builder.getMaintenanceExecutor();
    }

    /**
     * Creates a segment cache preloaded with delta cache entries.
     *
     * @return initialized segment cache
     */
    SegmentCache<K, V> createSegmentCache() {
        final SegmentCache<K, V> segmentCache = new SegmentCache<>(
                segmentFiles.getKeyTypeDescriptor().getComparator(),
                segmentFiles.getValueTypeDescriptor(), null,
                segmentConf.getMaxNumberOfKeysInSegmentWriteCache(),
                segmentConf
                        .getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(),
                segmentConf.getMaxNumberOfKeysInSegmentCache());
        new SegmentDeltaCacheLoader<>(segmentFiles, segmentPropertiesManager)
                .loadInto(segmentCache);
        return segmentCache;
    }

    /**
     * Determines the active segment version, falling back to the highest index
     * version found on disk when metadata is missing or stale.
     *
     * @param baseDirectory     base directory for the segment
     * @param layout            directory layout helper
     * @param propertiesManager properties manager for the segment
     * @return resolved active version
     */
    private long resolveActiveVersion(final AsyncDirectory baseDirectory,
            final SegmentDirectoryLayout layout,
            final SegmentPropertiesManager propertiesManager) {
        long activeVersion = propertiesManager.getVersion();
        if (activeVersion <= 0
                || !indexFileExists(baseDirectory, layout, activeVersion)) {
            final long detectedVersion = detectHighestIndexVersion(
                    baseDirectory, layout);
            activeVersion = detectedVersion >= 0 ? detectedVersion : 1L;
        }
        return activeVersion;
    }

    /**
     * Scans the directory for index files and returns the highest version.
     *
     * @param directory directory to inspect
     * @param layout    directory layout helper
     * @return highest index version, or -1 when none are present
     */
    private long detectHighestIndexVersion(final AsyncDirectory directory,
            final SegmentDirectoryLayout layout) {
        try (java.util.stream.Stream<String> files = directory
                .getFileNamesAsync().toCompletableFuture().join()) {
            return files.mapToLong(layout::parseVersionFromIndexFileName).max()
                    .orElse(-1L);
        }
    }

    /**
     * Checks whether the index file for a given version exists.
     *
     * @param directory directory to inspect
     * @param layout    directory layout helper
     * @param version   version to verify
     * @return true when the index file exists
     */
    private boolean indexFileExists(final AsyncDirectory directory,
            final SegmentDirectoryLayout layout, final long version) {
        return directory.isFileExistsAsync(layout.getIndexFileName(version))
                .toCompletableFuture().join();
    }

    /**
     * Ensures the properties manager reflects the active version and state.
     *
     * @param propertiesManager segment properties manager
     * @param activeVersion     active index version
     */
    private void initializeDirectoryMetadata(
            final SegmentPropertiesManager propertiesManager,
            final long activeVersion) {
        if (activeVersion >= 0
                && propertiesManager.getVersion() != activeVersion) {
            propertiesManager.setVersion(activeVersion);
        }
        propertiesManager
                .setState(SegmentPropertiesManager.SegmentDataState.ACTIVE);
    }

}
