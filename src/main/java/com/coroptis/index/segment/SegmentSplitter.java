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
        return new SegmentReader<>(segmentFiles, deltaCacheController)
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

//TODO some comment
    private final float MINIMAL_PERCENTAGE_DIFFERENCE = 90F;

    /**
     * When number keys in cache is just slightly smaller than number of keys in
     * main index than segment should be compacted before splitting.
     * 
     * When number of keys in main index and delta cache is similar and
     * operations in delta cache are deletes or updates than split operation
     * could create one segment empty.
     * 
     * FIXME write test for case when delta cache contains delete operations.
     * 
     * @return return <code>true</code> when segment should be compacted before
     *         splitting
     */
    public boolean souldBeCompacteBeforeSplitting() {
        final long countOfkeysInDeltaCache = getSegmentDeltaCache().size();
        final long countOfKeysInMainIndex = getStats().getNumberOfKeysInIndex();
        if (countOfKeysInMainIndex == 0) {
            return false;
        }
        final float onePercentageOfMainIndexKeyCount = countOfKeysInMainIndex
                / 100F;
        if (countOfkeysInDeltaCache
                / onePercentageOfMainIndexKeyCount > MINIMAL_PERCENTAGE_DIFFERENCE) {
            return true;
        } else {
            return false;
        }
    }

    public Result<K, V> split(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId);
        logger.debug("Splitting of '{}' started", segmentFiles.getId());
        versionController.changeVersion();
        /*
         * Real number of key is equals or lower than computed bellow. Keys in
         * cache could already be in main index file of it can be keys with
         * tombstone value.
         */
        final long estimatedNumberOfKeys = getStats().getNumberOfKeysInIndex()
                + getSegmentDeltaCache().size();
        final long half = estimatedNumberOfKeys / 2;

        final Segment<K, V> lowerSegment = Segment.<K, V>builder()
                .withDirectory(segmentFiles.getDirectory()).withId(segmentId)
                .withKeyTypeDescriptor(segmentFiles.getKeyTypeDescriptor())
                .withValueTypeDescriptor(segmentFiles.getValueTypeDescriptor())
                .withSegmentConf(segmentConf).build();

        K minKey = null;
        K maxKey = null;
        long cxLower = 0;
        long cxHigher = 0;
        try (final PairIterator<K, V> iterator = openIterator()) {

            try (final SegmentFullWriter<K, V> writer = lowerSegment
                    .openFullWriter()) {
                while (cxLower < half && iterator.hasNext()) {
                    cxLower++;
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
                    cxHigher++;
                }
            }

        }
        logger.debug("Splitting of '{}' finished, '{}' was created. "
                + "Estimated number of keys was {}, half key was {} and real number of keys was {}.",
                segmentFiles.getId(), lowerSegment.getId(),
                estimatedNumberOfKeys, half, cxLower + cxHigher);
        if (cxLower == 0) {
            throw new IllegalStateException(
                    "Splitting failed. Lower segment doesn't contains any data");
        }
        if (cxHigher == 0) {
            throw new IllegalStateException(
                    "Splitting failed. Higher segment doesn't contains any data");
        }
        return new Result<>(lowerSegment, minKey, maxKey);
    }

    private SegmentDeltaCache<K, V> getSegmentDeltaCache() {
        return deltaCacheController.getDeltaCache();
    }

}
