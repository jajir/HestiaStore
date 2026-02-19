package org.hestiastore.index.segment;

import java.util.concurrent.Executor;
import java.util.stream.Stream;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.directory.Directory;
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
        prepareCompaction(segment);
        final long currentVersion = Math.max(0,
                segment.getSegmentFiles().getActiveVersion());
        final long nextVersion = currentVersion + 1;
        return new CompactionPlan<>(segment, currentVersion,
                nextVersion);
    }

    /**
     * Captures a stable snapshot for compaction while the segment is frozen.
     *
     * @param segment segment core
     */
    public void prepareCompaction(final SegmentCore<K, V> segment) {
        Vldtn.requireNonNull(segment, "segment");
        logger.debug("Start of compacting '{}'", segment.getId());
        segment.resetSegmentIndexSearcher();
        segment.freezeWriteCacheForFlush();
    }

    void writeCompaction(final SegmentCore<K, V> segment,
            final SegmentFullWriterTx<K, V> writerTx) {
        Vldtn.requireNonNull(segment, "segment");
        Vldtn.requireNonNull(writerTx, "writerTx");
        try (EntryWriter<K, V> writer = writerTx.open();
                EntryIterator<K, V> iterator = segment
                        .openIteratorFromCompactionSnapshot()) {
            while (iterator.hasNext()) {
                writer.write(iterator.next());
            }
        }
    }

    void writeCompaction(final CompactionPlan<K, V> plan) {
        Vldtn.requireNonNull(plan, "plan");
        SegmentFullWriterTx<K, V> writerTx = plan.writerTx;
        if (writerTx == null) {
            writerTx = prepareVersionSwitch(plan);
        }
        writeCompaction(plan.segment, writerTx);
        writerTx.commit();
        finalizeVersionSwitch(plan);
    }

    /**
     * Publishes compaction results, updates version, and logs completion.
     *
     * @param segment segment core
     */
    void publishCompaction(final SegmentCore<K, V> segment) {
        Vldtn.requireNonNull(segment, "segment");
        versionController.changeVersion();
        logger.debug("End of compacting '{}'", segment.getId());
    }

    void publishCompaction(final CompactionPlan<K, V> plan) {
        Vldtn.requireNonNull(plan, "plan");
        applyVersionSwitch(plan);
        publishCompaction(plan.segment);
    }

    private SegmentFullWriterTx<K, V> prepareVersionSwitch(
            final CompactionPlan<K, V> plan) {
        final SegmentCore<K, V> segment = plan.segment;
        final SegmentFiles<K, V> segmentFiles = segment.getSegmentFiles();
        final SegmentFiles<K, V> preparedFiles = segmentFiles
                .copyWithVersion(plan.nextVersion);
        final SegmentResources<K, V> preparedResources = new SegmentResourcesImpl<>(
                new SegmentDataSupplier<>(preparedFiles,
                        segment.getSegmentConf()));
        final SegmentPropertiesManager propertiesManager = segment
                .getSegmentPropertiesManager();
        final SegmentFullWriterTx<K, V> writerTx = new SegmentFullWriterTx<>(
                preparedFiles, propertiesManager,
                segment.getSegmentConf().getMaxNumberOfKeysInChunk(),
                preparedResources, segment.getDeltaCacheController());
        plan.writerTx = writerTx;
        return writerTx;
    }

    private void finalizeVersionSwitch(final CompactionPlan<K, V> plan) {
        final SegmentPropertiesManager propertiesManager = plan.segment
                .getSegmentPropertiesManager();
        propertiesManager.setVersion(plan.nextVersion);
    }

    private void applyVersionSwitch(final CompactionPlan<K, V> plan) {
        plan.segment.switchActiveVersion(plan.nextVersion);
    }

    Runnable buildCleanupTask(final CompactionPlan<K, V> plan) {
        if (plan == null || plan.previousVersion == plan.nextVersion) {
            return null;
        }
        final SegmentDirectoryLayout layout = new SegmentDirectoryLayout(
                plan.segment.getId());
        final Directory directory = plan.segment.getSegmentFiles()
                .getDirectory();
        final long previousVersion = plan.previousVersion;
        return () -> cleanupOldVersion(directory, layout, previousVersion);
    }

    void scheduleCleanup(final CompactionPlan<K, V> plan,
            final Executor executor) {
        Vldtn.requireNonNull(executor, "executor");
        final Runnable cleanup = buildCleanupTask(plan);
        if (cleanup == null) {
            return;
        }
        try {
            executor.execute(cleanup);
        } catch (final RuntimeException e) {
            logger.warn("Failed to schedule cleanup for segment '{}'",
                    plan.segment.getId(), e);
        }
    }

    private void cleanupOldVersion(final Directory directory,
            final SegmentDirectoryLayout layout, final long version) {
        if (version < 0) {
            return;
        }
        deleteFile(directory, layout.getIndexFileName(version));
        deleteFile(directory, layout.getScarceFileName(version));
        deleteFile(directory, layout.getBloomFilterFileName(version));
        final String deltaPrefix = layout.getDeltaCachePrefix(version);
        try (Stream<String> files = directory.getFileNames()) {
            files.filter(name -> name.startsWith(deltaPrefix))
                    .forEach(name -> deleteFile(directory, name));
        }
    }

    private void deleteFile(final Directory directory,
            final String fileName) {
        try {
            directory.deleteFile(fileName);
        } catch (final RuntimeException e) {
            logger.warn("Failed to delete file '{}' in '{}'", fileName,
                    directory, e);
        }
    }

    static final class CompactionPlan<K, V> {
        private final SegmentCore<K, V> segment;
        private final long previousVersion;
        private final long nextVersion;
        private SegmentFullWriterTx<K, V> writerTx;

        private CompactionPlan(final SegmentCore<K, V> segment,
                final long previousVersion,
                final long nextVersion) {
            this.segment = segment;
            this.previousVersion = previousVersion;
            this.nextVersion = nextVersion;
        }
    }
}
