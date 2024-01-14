package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.PairIterator;
import com.coroptis.index.bloomfilter.BloomFilter;

public class SegmentCompacter<K, V> {

    private final SegmentConf segmentConf;
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentPropertiesController segmentPropertiesController;

    public SegmentCompacter(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController) {
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.versionController = Objects.requireNonNull(versionController,
                "Version controller is required");
        this.segmentPropertiesController = new SegmentPropertiesController(
                segmentFiles.getDirectory(), segmentFiles.getId(),
                versionController);
    }

    private PairIterator<K, V> openIterator() {
        // TODO this naive implementation ignores possible in memory cache.
        return new SegmentReader<>(segmentFiles,
                segmentPropertiesController.getSegmentPropertiesManager())
                .openIterator(versionController);
    }

    /**
     * 
     * @return return <code>true</code> when segment was compacted.
     */
    public boolean optionallyCompact() {
        final SegmentStats stats = segmentPropertiesController
                .getSegmentPropertiesManager().getSegmentStats();
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
        final SegmentStats stats = segmentPropertiesController
                .getSegmentPropertiesManager().getSegmentStats();
        return stats.getNumberOfKeysInCache()
                + numberOfKeysInLastDeltaFile > segmentConf
                        .getMaxNumberOfKeysInSegmentCache();
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
        versionController.changeVersion();
        try (final SegmentFullWriter<K, V> writer = openFullWriter()) {
            try (final PairIterator<K, V> iterator = openIterator()) {
                while (iterator.hasNext()) {
                    writer.put(iterator.next());
                }
            }
        }
    }

    /**
     * Method should be called just from inside of this package. Method open
     * direct writer to scarce index and main sst file. It's useful for
     * compacting.
     */
    private SegmentFullWriter<K, V> openFullWriter() {
        final BloomFilter<K> bloomFilter = BloomFilter.<K>builder()
                .withBloomFilterFileName(segmentFiles.getBloomFilterFileName())
                .withConvertorToBytes(segmentFiles.getKeyTypeDescriptor()
                        .getConvertorToBytes())
                .withDirectory(segmentFiles.getDirectory())
                .withIndexSizeInBytes(
                        segmentConf.getBloomFilterIndexSizeInBytes())
                .withNumberOfHashFunctions(
                        segmentConf.getBloomFilterNumberOfHashFunctions())
                .build();
        return new SegmentFullWriter<K, V>(bloomFilter, segmentFiles,
                segmentPropertiesController,
                segmentConf.getMaxNumberOfKeysInIndexPage());
    }
}
