package com.coroptis.index.segment;

import java.util.Objects;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.bloomfilter.BloomFilter;

/**
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentSplitter<K, V> {

    private final SegmentConf segmentConf;
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentStatsController segmentStatsController;

    public SegmentSplitter(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController) {
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.versionController = Objects.requireNonNull(versionController,
                "Version controller is required");
        this.segmentStatsController = new SegmentStatsController(
                segmentFiles.getDirectory(), segmentFiles.getId(),
                versionController);
    }

    private SegmentStats getStats() {
        return segmentStatsController.getSegmentStatsManager()
                .getSegmentStats();
    }

    private PairIterator<K, V> openIterator() {
        // TODO this naive implementation ignores possible in memory cache.
        return new SegmentReader<>(segmentFiles)
                .openIterator(versionController);
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
                segmentStatsController,
                segmentConf.getMaxNumberOfKeysInIndexPage());
    }

    public Segment<K, V> split(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId);
        versionController.changeVersion();
        long cx = 0;
        long half = getStats().getNumberOfKeys() / 2;

        final Segment<K, V> lowerSegment = Segment.<K, V>builder()
                .withDirectory(segmentFiles.getDirectory()).withId(segmentId)
                .withKeyTypeDescriptor(segmentFiles.getKeyTypeDescriptor())
                .withValueTypeDescriptor(segmentFiles.getValueTypeDescriptor())
                .withSegmentConf(segmentConf).build();

        try (final PairIterator<K, V> iterator = openIterator()) {

            try (final SegmentFullWriter<K, V> writer = lowerSegment
                    .openFullWriter()) {
                while (cx < half && iterator.hasNext()) {
                    cx++;
                    final Pair<K, V> pair = iterator.next();
                    writer.put(pair);
                }
            }

            try (final SegmentFullWriter<K, V> writer = openFullWriter()) {
                while (iterator.hasNext()) {
                    final Pair<K, V> pair = iterator.next();
                    writer.put(pair);
                }
            }

        }

        return lowerSegment;
    }

}
