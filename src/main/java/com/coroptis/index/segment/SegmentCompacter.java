package com.coroptis.index.segment;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.PairIterator;

public class SegmentCompacter<K, V> {

    private final Logger logger = LoggerFactory
            .getLogger(SegmentCompacter.class);
    private final Segment<K, V> segment;
    private final SegmentConf segmentConf;
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentPropertiesManager segmentPropertiesManager;

    public SegmentCompacter(final Segment<K, V> segment, final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager) {
        this.segment = Objects.requireNonNull(segment);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.versionController = Objects.requireNonNull(versionController,
                "Version controller is required");
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
    }

    /**
     * 
     * @return return <code>true</code> when segment was compacted.
     */
    public boolean optionallyCompact() {
        final SegmentStats stats = segmentPropertiesManager.getSegmentStats();
        if (stats.getNumberOfKeysInCache() > segmentConf
                .getMaxNumberOfKeysInSegmentCache()) {
            forceCompact();
            return true;
        }
        return false;
    }

    /**
     * Provide information if segment should be compacted. Method doesn't
     * perform compact operation.
     * 
     * @return return <code>true</code> when segment should be compacted
     */
    public boolean shouldBeCompacted(final long i) {
        final SegmentStats stats = segmentPropertiesManager.getSegmentStats();
        return stats.getNumberOfKeysInCache()
                 > segmentConf
                        .getMaxNumberOfKeysInSegmentCache();
    }

    /**
     * Provide information if segment should be compacted. Method doesn't
     * perform compact operation.
     * 
     * Method should be used during flushing data from main cache. In that case
     * could be in cache delta index mode data that is total limit. It prevent
     * index from repeating compacting.
     * 
     * @param numberOfKeysInLastDeltaFile required number of keys in last delta
     *                                    cache file
     * @return return <code>true</code> when segment should be compacted even
     *         during flushing.
     */
    public boolean shouldBeCompactedDuringFlushing(
            final long numberOfKeysInLastDeltaFile) {
        final SegmentStats stats = segmentPropertiesManager.getSegmentStats();
        return stats.getNumberOfKeysInCache()
                + numberOfKeysInLastDeltaFile > segmentConf
                        .getMaxNumberOfKeysInSegmentCacheDuringFlushing();
    }

    public boolean shouldBeCompactedDuringWriting(
            final long numberOfKeysInLastDeltaFile) {
        final SegmentStats stats = segmentPropertiesManager.getSegmentStats();
        return stats.getNumberOfKeysInCache()
                + numberOfKeysInLastDeltaFile > segmentConf
                        .getMaxNumberOfKeysInSegmentCache();
                    //FIXME remove getMaxNumberOfKeysInSegmentMemory, it's useless
    }

    /**
     * 
     * @return return <code>true</code> when segment was compacted.
     */
    @Deprecated
    public boolean optionallyCompact(final long numberOfKeysInLastDeltaFile) {
        if (shouldBeCompacted(0)) {
            forceCompact();
            return true;
        }
        return false;
    }

    public void forceCompact() {
        logger.debug("Start of compacting '{}'", segmentFiles.getId());
        versionController.changeVersion();
        try (SegmentFullWriter<K, V> writer = segment.openFullWriter()) {
            try (PairIterator<K, V> iterator = segment.openIterator()) {
                while (iterator.hasNext()) {
                    writer.put(iterator.next());
                }
            }
        }
        logger.debug("End of compacting '{}'", segmentFiles.getId());
    }

}
