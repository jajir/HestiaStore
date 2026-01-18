package org.hestiastore.index.segment;

import java.util.List;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.async.AsyncDirectory;
import org.hestiastore.index.segment.SegmentPropertiesManager.SegmentDataState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class is responsible for compacting segment. It also verify if segment should
 * be compacted.
 */
final class SegmentCompacter<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final VersionController versionController;

    /**
     * Creates a compacter that updates the segment version on publish.
     *
     * @param versionController version controller for iterator invalidation
     */
    public SegmentCompacter(final VersionController versionController) {
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
    }

    /**
     * Prepares a compaction plan that captures the snapshot and wiring.
     *
     * @param segment segment core
     * @return compaction plan
     */
    CompactionPlan<K, V> prepareCompactionPlan(
            final SegmentCore<K, V> segment) {
        Vldtn.requireNonNull(segment, "segment");
        final List<Entry<K, V>> snapshotEntries = prepareCompaction(segment);
        final SegmentFiles<K, V> segmentFiles = segment.getSegmentFiles();
        if (!segmentFiles.isSegmentRootDirectoryEnabled()) {
            final SegmentFullWriterTx<K, V> writerTx = segment.openFullWriteTx();
            return new CompactionPlan<>(segment, snapshotEntries, writerTx, 0L);
        }
        final long currentVersion = Math.max(0,
                segment.getSegmentPropertiesManager().getVersion());
        final long nextVersion = currentVersion + 1;
        return new CompactionPlan<>(segment, snapshotEntries, null,
                nextVersion);
    }

    /**
     * Captures a stable snapshot for compaction while the segment is frozen.
     *
     * @param segment segment core
     * @return sorted snapshot entries
     */
    public List<Entry<K, V>> prepareCompaction(final SegmentCore<K, V> segment) {
        Vldtn.requireNonNull(segment, "segment");
        logger.debug("Start of compacting '{}'", segment.getId());
        segment.resetSegmentIndexSearcher();
        segment.freezeWriteCacheForFlush();
        return segment.snapshotCacheEntries();
    }

    /**
     * Performs compaction using a previously captured snapshot.
     *
     * @param segment segment core
     * @param snapshotEntries snapshot entries captured in FREEZE
     */
    public void compact(final SegmentCore<K, V> segment,
            final List<Entry<K, V>> snapshotEntries) {
        Vldtn.requireNonNull(segment, "segment");
        Vldtn.requireNonNull(snapshotEntries, "snapshotEntries");
        final SegmentFullWriterTx<K, V> writerTx = segment.openFullWriteTx();
        writeCompaction(segment, snapshotEntries, writerTx);
        publishCompaction(segment, writerTx);
    }

    void writeCompaction(final SegmentCore<K, V> segment,
            final List<Entry<K, V>> snapshotEntries,
            final SegmentFullWriterTx<K, V> writerTx) {
        Vldtn.requireNonNull(segment, "segment");
        Vldtn.requireNonNull(snapshotEntries, "snapshotEntries");
        Vldtn.requireNonNull(writerTx, "writerTx");
        try (EntryWriter<K, V> writer = writerTx.open();
                EntryIterator<K, V> iterator = segment
                        .openIteratorFromSnapshot(snapshotEntries)) {
            while (iterator.hasNext()) {
                writer.write(iterator.next());
            }
        }
    }

    void writeCompaction(final CompactionPlan<K, V> plan) {
        Vldtn.requireNonNull(plan, "plan");
        SegmentFullWriterTx<K, V> writerTx = plan.writerTx;
        if (writerTx == null) {
            writerTx = prepareDirectorySwitch(plan);
        }
        writeCompaction(plan.segment, plan.snapshotEntries, writerTx);
    }

    /**
     * Commits compaction results, updates version, and logs completion.
     *
     * @param segment segment core
     * @param writerTx full writer transaction to commit
     */
    void publishCompaction(final SegmentCore<K, V> segment,
            final SegmentFullWriterTx<K, V> writerTx) {
        Vldtn.requireNonNull(segment, "segment");
        Vldtn.requireNonNull(writerTx, "writerTx");
        writerTx.commit();
        versionController.changeVersion();
        logger.debug("End of compacting '{}'", segment.getId());
    }

    void publishCompaction(final CompactionPlan<K, V> plan) {
        Vldtn.requireNonNull(plan, "plan");
        publishCompaction(plan.segment, plan.writerTx);
        if (plan.directorySwitch != null) {
            applyDirectorySwitch(plan);
        }
    }

    /**
     * Runs compaction end-to-end using a fresh snapshot.
     *
     * @param segment segment core
     */
    public void forceCompact(final SegmentCore<K, V> segment) {
        final List<Entry<K, V>> snapshotEntries = prepareCompaction(segment);
        compact(segment, snapshotEntries);
    }

    private SegmentFullWriterTx<K, V> prepareDirectorySwitch(
            final CompactionPlan<K, V> plan) {
        final SegmentCore<K, V> segment = plan.segment;
        final SegmentFiles<K, V> segmentFiles = segment.getSegmentFiles();
        final String activeDirectoryName = segmentFiles.getActiveDirectoryName();
        if (activeDirectoryName == null || activeDirectoryName.isBlank()) {
            throw new IllegalStateException(
                    "Active directory name is missing for directory layout.");
        }
        final long version = plan.nextVersion;
        final String preparedDirectoryName = SegmentDirectoryLayout
                .getVersionDirectoryName(version);
        final AsyncDirectory rootDirectory = segmentFiles.getRootDirectory();
        final AsyncDirectory preparedDirectory = rootDirectory
                .openSubDirectory(preparedDirectoryName)
                .toCompletableFuture().join();
        clearDirectory(preparedDirectory, null);
        final SegmentFiles<K, V> preparedFiles = segmentFiles
                .copyWithDirectory(preparedDirectoryName, preparedDirectory);
        final SegmentPropertiesManager preparedProperties = new SegmentPropertiesManager(
                preparedDirectory, segmentFiles.getId());
        preparedProperties.setVersion(version);
        preparedProperties.setState(SegmentDataState.PREPARED);
        final SegmentResources<K, V> preparedResources = new SegmentResourcesImpl<>(
                new SegmentDataSupplier<>(preparedFiles,
                        segment.getSegmentConf()));
        final SegmentFullWriterTx<K, V> writerTx = new SegmentFullWriterTx<>(
                preparedFiles, preparedProperties,
                segment.getSegmentConf().getMaxNumberOfKeysInChunk(),
                preparedResources, segment.getDeltaCacheController());
        plan.writerTx = writerTx;
        plan.directorySwitch = new DirectorySwitch<>(rootDirectory,
                activeDirectoryName, preparedDirectoryName, preparedDirectory,
                preparedProperties);
        return writerTx;
    }

    private void applyDirectorySwitch(final CompactionPlan<K, V> plan) {
        final DirectorySwitch<K, V> directorySwitch = plan.directorySwitch;
        directorySwitch.preparedProperties.setState(SegmentDataState.ACTIVE);
        final SegmentDirectoryLayout layout = new SegmentDirectoryLayout(
                plan.segment.getId());
        final SegmentDirectoryPointer pointer = new SegmentDirectoryPointer(
                directorySwitch.rootDirectory, layout);
        pointer.writeActiveDirectory(directorySwitch.preparedDirectoryName);
        plan.segment.switchActiveDirectory(
                directorySwitch.preparedDirectoryName,
                directorySwitch.preparedDirectory);
        cleanupOldDirectory(directorySwitch, layout.getActivePointerFileName(),
                plan.segment.getId());
    }

    private void cleanupOldDirectory(final DirectorySwitch<K, V> directorySwitch,
            final String pointerFileName, final SegmentId segmentId) {
        if (directorySwitch.activeDirectoryName
                .equals(directorySwitch.preparedDirectoryName)) {
            return;
        }
        if (SegmentDirectoryLayout.ROOT_DIRECTORY_NAME
                .equals(directorySwitch.activeDirectoryName)) {
            clearDirectory(directorySwitch.rootDirectory, pointerFileName);
            return;
        }
        try {
            final AsyncDirectory oldDirectory = directorySwitch.rootDirectory
                    .openSubDirectory(directorySwitch.activeDirectoryName)
                    .toCompletableFuture().join();
            clearDirectory(oldDirectory, null);
            directorySwitch.rootDirectory
                    .rmdir(directorySwitch.activeDirectoryName)
                    .toCompletableFuture().join();
        } catch (final RuntimeException e) {
            logger.warn("Failed to remove old directory '{}' for segment '{}'",
                    directorySwitch.activeDirectoryName, segmentId, e);
        }
    }

    private void clearDirectory(final AsyncDirectory directoryFacade,
            final String skipFileName) {
        try (Stream<String> files = directoryFacade.getFileNamesAsync()
                .toCompletableFuture().join()) {
            files.forEach(name -> {
                if (skipFileName != null && skipFileName.equals(name)) {
                    return;
                }
                try {
                    directoryFacade.deleteFileAsync(name)
                            .toCompletableFuture().join();
                } catch (final RuntimeException e) {
                    logger.warn("Failed to delete file '{}' in '{}'", name,
                            directoryFacade, e);
                }
            });
        }
    }

    static final class CompactionPlan<K, V> {
        private final SegmentCore<K, V> segment;
        private final List<Entry<K, V>> snapshotEntries;
        private final long nextVersion;
        private SegmentFullWriterTx<K, V> writerTx;
        private DirectorySwitch<K, V> directorySwitch;

        private CompactionPlan(final SegmentCore<K, V> segment,
                final List<Entry<K, V>> snapshotEntries,
                final SegmentFullWriterTx<K, V> writerTx,
                final long nextVersion) {
            this.segment = segment;
            this.snapshotEntries = snapshotEntries;
            this.writerTx = writerTx;
            this.nextVersion = nextVersion;
        }
    }

    private static final class DirectorySwitch<K, V> {
        private final AsyncDirectory rootDirectory;
        private final String activeDirectoryName;
        private final String preparedDirectoryName;
        private final AsyncDirectory preparedDirectory;
        private final SegmentPropertiesManager preparedProperties;

        private DirectorySwitch(final AsyncDirectory rootDirectory,
                final String activeDirectoryName,
                final String preparedDirectoryName,
                final AsyncDirectory preparedDirectory,
                final SegmentPropertiesManager preparedProperties) {
            this.rootDirectory = rootDirectory;
            this.activeDirectoryName = activeDirectoryName;
            this.preparedDirectoryName = preparedDirectoryName;
            this.preparedDirectory = preparedDirectory;
            this.preparedProperties = preparedProperties;
        }
    }
}
