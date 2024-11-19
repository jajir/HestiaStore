package com.coroptis.index.segment;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coroptis.index.F;
import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;

/**
 * 
 * @param <K>
 * @param <V>
 */
public class SegmentSplitter<K, V> {

    private final Logger logger = LoggerFactory
            .getLogger(SegmentSplitter.class);
    private final Segment<K, V> segment;
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final SegmentManager<K, V> segmentManager;

    public SegmentSplitter(final Segment<K, V> segment,
            final SegmentFiles<K, V> segmentFiles,
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentDeltaCacheController<K, V> deltaCacheController,
            final SegmentManager<K, V> segmentManager) {
        this.segment = Objects.requireNonNull(segment);
        this.segmentFiles = Objects.requireNonNull(segmentFiles);
        this.versionController = Objects.requireNonNull(versionController,
                "Version controller is required");
        this.segmentPropertiesManager = Objects
                .requireNonNull(segmentPropertiesManager);
        this.deltaCacheController = Objects
                .requireNonNull(deltaCacheController);
        this.segmentManager = Objects.requireNonNull(segmentManager);
    }

    private SegmentStats getStats() {
        return segmentPropertiesManager.getSegmentStats();
    }

    private SegmentFullWriter<K, V> openFullWriter() {
        return segmentManager.createSegmentFullWriter();
    }

    private final float MINIMAL_PERCENTAGE_DIFFERENCE = 90F;

    public boolean shouldBeCompactedBeforeSplitting() {
        final long countOfkeysInDeltaCache = deltaCacheController
                .getDeltaCacheSize();
        final long countOfKeysInMainIndex = getStats().getNumberOfKeysInIndex();
        if (countOfKeysInMainIndex == 0) {
            return false;
        }
        final float onePercentageOfMainIndexKeyCount = countOfKeysInMainIndex
                / 100F;
        return countOfkeysInDeltaCache
                / onePercentageOfMainIndexKeyCount > MINIMAL_PERCENTAGE_DIFFERENCE;
    }

    public SegmentSplitterResult<K, V> split(final SegmentId segmentId) {
        Objects.requireNonNull(segmentId, "Segment id is required");
        logger.debug("Splitting of '{}' started", segmentFiles.getId());
        versionController.changeVersion();
        /*
         * Real number of key is equals or lower than computed bellow. Keys in
         * cache could already be in main index file of it can be keys with
         * tombstone value.
         */
        final long estimatedNumberOfKeys = getStats().getNumberOfKeysInIndex()
                + deltaCacheController.getDeltaCacheSize();
        final long half = estimatedNumberOfKeys / 2;

        final Segment<K, V> lowerSegment = segmentManager.createSegment(segmentId);

        K minKey = null;
        K maxKey = null;
        long cxLower = 0;
        long cxHigher = 0;
        try (final PairIterator<K, V> iterator = segment.openIterator()) {

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
        logger.debug(
                "Splitting of '{}' finished, '{}' was created. "
                        + "Estimated number of keys was '{}', "
                        + "half key was '{}' and real number of keys was '{}'.",
                segmentFiles.getId(), lowerSegment.getId(),
                F.fmt(estimatedNumberOfKeys), F.fmt(half),
                F.fmt(cxLower + cxHigher));
        if (cxLower == 0) {
            throw new IllegalStateException(
                    "Splitting failed. Lower segment doesn't contains any data");
        }
        if (cxHigher == 0) {
            throw new IllegalStateException(
                    "Splitting failed. Higher segment doesn't contains any data");
        }
        return new SegmentSplitterResult<>(lowerSegment, minKey, maxKey);
    }

}
