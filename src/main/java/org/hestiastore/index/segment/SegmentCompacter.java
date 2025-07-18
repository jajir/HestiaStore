package org.hestiastore.index.segment;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.Vldtn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class is responsible for compacting segment. It also verify if segment should
 * be compacted.
 */
public final class SegmentCompacter<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Segment<K, V> segment;
    private final SegmentConf segmentConf;
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentPropertiesManager segmentPropertiesManager;

    public SegmentCompacter(final Segment<K, V> segment,
            final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager) {
        this.segment = Vldtn.requireNonNull(segment, "segment");
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.segmentConf = Vldtn.requireNonNull(segmentConf, "segmentConf");
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
    }

    /**
     * Optionally compact segment. Method check if segment should be compacted
     * and if should be than it compact it.
     * 
     * @return return <code>true</code> when segment was compacted.
     */
    public boolean optionallyCompact() {
        if (shouldBeCompacted()) {
            forceCompact();
            return true;
        }
        return false;
    }

    /**
     * Provide information if segment should be compacted. Method doesn't load
     * segment data intomemory.
     * 
     * @return return <code>true</code> when segment should be compacted
     */
    public boolean shouldBeCompacted() {
        final SegmentStats stats = segmentPropertiesManager.getSegmentStats();
        return stats.getNumberOfKeysInDeltaCache() > segmentConf
                .getMaxNumberOfKeysInDeltaCache();
    }

    /**
     * Provide information if segment should be compacted.
     * 
     * Method should be used during writing data to segment. During writing to
     * segmrnt is's reasonable to have more datat in deta chache than usually
     * and compact once after all data writing.
     * 
     * @param numberOfKeysInLastDeltaFile required number of keys in last delta
     *                                    cache file
     * @return return <code>true</code> when segment should be compacted even
     *         during writing.
     */
    public boolean shouldBeCompactedDuringWriting(
            final long numberOfKeysInLastDeltaFile) {
        final SegmentStats stats = segmentPropertiesManager.getSegmentStats();
        return stats.getNumberOfKeysInDeltaCache()
                + numberOfKeysInLastDeltaFile > segmentConf
                        .getMaxNumberOfKeysInDeltaCacheDuringWriting();
    }

    public void forceCompact() {
        logger.debug("Start of compacting '{}'", segmentFiles.getId());
        versionController.changeVersion();
        segment.executeFullWriteTx(writer -> {
            try (PairIterator<K, V> iterator = segment.openIterator()) {
                Pair<K, V> pair;
                while (iterator.hasNext()) {
                    pair = iterator.next();
                    writer.put(pair);
                }
            }
        });
        logger.debug("End of compacting '{}'", segmentFiles.getId());
    }

}
