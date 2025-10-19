package org.hestiastore.index.segment;

import org.hestiastore.index.Vldtn;

/**
 * Applies the result of a split when the upper part is empty, effectively
 * replacing the current segment with the lower segment content by renaming
 * files and updating caches and stats.
 */
final class SegmentReplacer<K, V> {

    private final SegmentFilesRenamer filesRenamer;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentFiles<K, V> targetSegmentFiles;

    SegmentReplacer(final SegmentFilesRenamer filesRenamer,
            final SegmentDeltaCacheController<K, V> deltaCacheController,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentFiles<K, V> targetSegmentFiles) {
        this.filesRenamer = Vldtn.requireNonNull(filesRenamer, "filesRenamer");
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.targetSegmentFiles = Vldtn.requireNonNull(targetSegmentFiles,
                "targetSegmentFiles");
    }

    void replaceWithLower(final Segment<K, V> lowerSegment) {
        Vldtn.requireNonNull(lowerSegment, "lowerSegment");
        filesRenamer.renameFiles(lowerSegment.getSegmentFiles(),
                targetSegmentFiles);

        deltaCacheController.clear();

        segmentPropertiesManager.setNumberOfKeysInCache(0);
        final SegmentStats stats = lowerSegment.getSegmentPropertiesManager()
                .getSegmentStats();
        segmentPropertiesManager
                .setNumberOfKeysInIndex(stats.getNumberOfKeysInSegment());
        segmentPropertiesManager.setNumberOfKeysInScarceIndex(
                stats.getNumberOfKeysInScarceIndex());
        segmentPropertiesManager.flush();
    }
}
