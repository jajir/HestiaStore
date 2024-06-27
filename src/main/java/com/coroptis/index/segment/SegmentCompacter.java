package com.coroptis.index.segment;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.PairIterator;

public class SegmentCompacter<K, V> {

    private final Logger logger = LoggerFactory
            .getLogger(SegmentCompacter.class);
    private final SegmentConf segmentConf;
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentDataProvider<K, V> segmentCacheDataProvider;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;

    public SegmentCompacter(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentDataProvider<K, V> segmentCacheDataProvider,
            final SegmentDeltaCacheController<K, V> deltaCacheController) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.versionController = Objects.requireNonNull(versionController,
                "Version controller is required");
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
        this.segmentCacheDataProvider = Objects.requireNonNull(
                segmentCacheDataProvider,
                "Segment cached data provider is required");
        this.deltaCacheController = Objects.requireNonNull(deltaCacheController,
                "Delta cache controller is required");
    }

    private PairIterator<K, V> openIterator() {
        return new SegmentReader<>(segmentFiles, deltaCacheController)
                .openIterator(versionController);
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
     * @param numberOfKeysInLastDeltaFile required number of keys in last delta
     *                                    cache file
     * @return return <code>true</code> when segment should be compacted
     */
    public boolean shouldBeCompacted(final long numberOfKeysInLastDeltaFile) {
        final SegmentStats stats = segmentPropertiesManager.getSegmentStats();
        return stats.getNumberOfKeysInCache()
                + numberOfKeysInLastDeltaFile > segmentConf
                        .getMaxNumberOfKeysInSegmentCache();
    }

    public boolean shouldBeCompactedDuringWriting(
            final long numberOfKeysInLastDeltaFile) {
        final SegmentStats stats = segmentPropertiesManager.getSegmentStats();
        return stats.getNumberOfKeysInCache()
                + numberOfKeysInLastDeltaFile > segmentConf
                        .getMaxNumberOfKeysInSegmentMemory();
    }

    /**
     * 
     * @return return <code>true</code> when segment was compacted.
     */
    public boolean optionallyCompact(final long numberOfKeysInLastDeltaFile) {
        if (shouldBeCompacted(numberOfKeysInLastDeltaFile)) {
            forceCompact();
            return true;
        }
        return false;
    }

    public void forceCompact() {
        logger.debug("Start of compacting '{}'", segmentFiles.getId());
        versionController.changeVersion();
        try (SegmentFullWriter<K, V> writer = openFullWriter()) {
            try (PairIterator<K, V> iterator = openIterator()) {
                while (iterator.hasNext()) {
                    writer.put(iterator.next());
                }
            }
        }
        logger.debug("End of compacting '{}'", segmentFiles.getId());
    }

    /**
     * Method should be called just from inside of this package. Method open
     * direct writer to scarce index and main sst file. It's useful for
     * compacting.
     */
    private SegmentFullWriter<K, V> openFullWriter() {
        return new SegmentFullWriter<K, V>(segmentFiles,
                segmentPropertiesManager,
                segmentConf.getMaxNumberOfKeysInIndexPage(),
                segmentCacheDataProvider, deltaCacheController);
    }
}
