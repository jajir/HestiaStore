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
    private final SegmentPropertiesController segmentPropertiesController;

    public SegmentSplitter(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController) {
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.versionController = Objects.requireNonNull(versionController,
                "Version controller is required");
        this.segmentPropertiesController = new SegmentPropertiesController(
                segmentFiles.getDirectory(), segmentFiles.getId(),
                versionController);
    }

    private SegmentStats getStats() {
        return segmentPropertiesController.getSegmentPropertiesManager()
                .getSegmentStats();
    }

    private PairIterator<K, V> openIterator() {
        return new SegmentReader<>(segmentFiles,
                segmentPropertiesController.getSegmentPropertiesManager())
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
                segmentPropertiesController,
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
