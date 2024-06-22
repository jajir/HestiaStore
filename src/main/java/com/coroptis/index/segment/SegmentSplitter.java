package com.coroptis.index.segment;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;

/**
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class SegmentSplitter<K, V> {

    private final Logger logger = LoggerFactory
            .getLogger(SegmentSplitter.class);
    private final SegmentConf segmentConf;
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentCacheDataProvider<K, V> segmentCacheDataProvider;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;

    public static class Result<K, V> {

        private final Segment<K, V> segment;
        private final K maxKey;
        private final K minKey;

        private Result(final Segment<K, V> segment, final K minKey,
                final K maxKey) {
            this.segment = Objects.requireNonNull(segment);
            this.minKey = Objects.requireNonNull(minKey);
            this.maxKey = Objects.requireNonNull(maxKey);
        }

        public Segment<K, V> getSegment() {
            return segment;
        }

        public K getMaxKey() {
            return maxKey;
        }

        public K getMinKey() {
            return minKey;
        }

    }

    public SegmentSplitter(final SegmentFiles<K, V> segmentFiles,
            final SegmentConf segmentConf,
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentCacheDataProvider<K, V> segmentCacheDataProvider,
            final SegmentDeltaCacheController<K, V> deltaCacheController) {
        this.segmentConf = Objects.requireNonNull(segmentConf);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.versionController = Objects.requireNonNull(versionController,
                "Version controller is required");
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
        this.segmentCacheDataProvider = Objects.requireNonNull(
                segmentCacheDataProvider,
                "Segment cached data provider is required");
        this.deltaCacheController = Objects
                .requireNonNull(deltaCacheController);
    }

    private SegmentStats getStats() {
        return segmentPropertiesManager.getSegmentStats();
    }

    private PairIterator<K, V> openIterator() {
        return new SegmentReader<>(segmentFiles, segmentCacheDataProvider)
                .openIterator(versionController);
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

    public Result<K, V> split(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId);
        logger.debug("Start of splitting '{}'", segmentFiles.getId());
        versionController.changeVersion();
        long cx = 0;
        /*
         * Real number of key is equals or lower than computed bellow. Keys in
         * cache could already be in main index file of it can be keys with
         * tombstone value.
         */
        final SegmentDeltaCache<K, V> sc = new SegmentDeltaCache<>(
                segmentFiles.getKeyTypeDescriptor(), segmentFiles,
                segmentPropertiesManager);
        long half = (getStats().getNumberOfKeysInIndex() + sc.size()) / 2;

        final Segment<K, V> lowerSegment = Segment.<K, V>builder()
                .withDirectory(segmentFiles.getDirectory()).withId(segmentId)
                .withKeyTypeDescriptor(segmentFiles.getKeyTypeDescriptor())
                .withValueTypeDescriptor(segmentFiles.getValueTypeDescriptor())
                .withSegmentConf(segmentConf).build();

        K minKey = null;
        K maxKey = null;
        try (final PairIterator<K, V> iterator = openIterator()) {

            try (final SegmentFullWriter<K, V> writer = lowerSegment
                    .openFullWriter()) {
                while (cx < half && iterator.hasNext()) {
                    cx++;
                    final Pair<K, V> pair = iterator.next();
                    if (minKey == null) {
                        minKey = pair.getKey();
                    }
                    maxKey = pair.getKey();
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
        logger.debug("End of splitting '{}', '{}' was created",
                segmentFiles.getId(), lowerSegment.getId());
        return new Result<>(lowerSegment, minKey, maxKey);
    }

}
