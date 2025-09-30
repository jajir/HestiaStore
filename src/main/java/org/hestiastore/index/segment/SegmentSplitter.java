package org.hestiastore.index.segment;

import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.F;
import org.hestiastore.index.AtomicKey;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairWriter;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.WriteTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @param <K>
 * @param <V>
 */
public class SegmentSplitter<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
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
        this.segment = Vldtn.requireNonNull(segment, "segment");
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
        this.segmentManager = Vldtn.requireNonNull(segmentManager,
                "segmentManager");
    }

    private SegmentStats getStats() {
        return segmentPropertiesManager.getSegmentStats();
    }

    private static final float MINIMAL_PERCENTAGE_DIFFERENCE = 0.9F;

    /**
     * Method checks if segment should be compacted before splitting. It prevent
     * situation when delta cache is full of thombstones and because of that
     * segment is not eligible for splitting.
     * 
     * It lead to loading of delta cache into memory.
     * 
     * @return Return <code>true</code> if segment should be compacted before
     *         splitting.
     */
    public boolean shouldBeCompactedBeforeSplitting(
            long maxNumberOfKeysInSegment) {
        final long estimatedNumberOfKeys = getEstimatedNumberOfKeys();
        if (estimatedNumberOfKeys <= 3) {
            return true;
        }

        /**
         * It it's true than it seems that number of keys in segment after
         * compacting will be lower about 10% to maximam allowed number of key
         * in segment. So splitting is not necessary.
         */
        return estimatedNumberOfKeys < maxNumberOfKeysInSegment
                * MINIMAL_PERCENTAGE_DIFFERENCE;
    }

    public SegmentSplitterResult<K, V> split(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        logger.debug("Splitting of '{}' started", segmentFiles.getId());
        versionController.changeVersion();

        final long estimatedNumberOfKeys = getEstimatedNumberOfKeys();
        final long half = estimatedNumberOfKeys / 2;
        if (half <= 1) {
            throw new IllegalStateException(
                    "Splitting failed. Number of keys is too low.");
        }
        final Segment<K, V> lowerSegment = segmentManager
                .createSegment(segmentId);

        final AtomicKey<K> minKey = new AtomicKey<>();
        final AtomicKey<K> maxKey = new AtomicKey<>();
        final AtomicLong cxLower = new AtomicLong(0);
        final AtomicLong cxHigher = new AtomicLong(0);
        try (PairIterator<K, V> iterator = segment.openIterator()) {
            final WriteTransaction<K, V> lowerSegmentWriteTx = lowerSegment
                    .openFullWriteTx();
            try (PairWriter<K, V> writer = lowerSegmentWriteTx.openWriter()) {
                while (cxLower.get() < half && iterator.hasNext()) {
                    cxLower.incrementAndGet();
                    final Pair<K, V> pair = iterator.next();
                    if (minKey.isEmpty()) {
                        minKey.set(pair.getKey());
                    }
                    maxKey.set(pair.getKey());
                    writer.write(pair);
                }
            }
            lowerSegmentWriteTx.commit();

            if (cxLower.get() == 0) {
                throw new IllegalStateException(
                        "Splitting failed. Lower segment doesn't contains any data");
            }

            if (iterator.hasNext()) {
                /**
                 * There are some more keys in segment, so split the segment.
                 */
                final WriteTransaction<K, V> segmentWriteTx = segment
                        .openFullWriteTx();
                try (PairWriter<K, V> writer = segmentWriteTx.openWriter()) {
                    while (iterator.hasNext()) {
                        final Pair<K, V> pair = iterator.next();
                        writer.write(pair);
                        cxHigher.incrementAndGet();
                    }
                }
                segmentWriteTx.commit();

                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Splitting of '{}' finished, '{}' was created. "
                                    + "Estimated number of keys was '{}', "
                                    + "half key was '{}' and real number of keys was '{}'.",
                            segmentFiles.getId(), lowerSegment.getId(),
                            F.fmt(estimatedNumberOfKeys), F.fmt(half),
                            F.fmt(cxLower.get() + cxHigher.get()));
                }
                if (cxHigher.get() == 0) {
                    throw new IllegalStateException(String.format(
                            "Splitting failed. Higher segment doesn't contains any data. Estimated number of keys was '%s'",
                            F.fmt(estimatedNumberOfKeys)));
                }
                return new SegmentSplitterResult<>(lowerSegment, minKey.get(),
                        maxKey.get(),
                        SegmentSplitterResult.SegmentSplittingStatus.SPLITED);
            } else {
                /**
                 * There are no more keys in segment, so just compact segment.
                 * 
                 * Data from lower segment have to be moved to current one.
                 */

                // Moving segment main data file
                segmentFiles.getDirectory().renameFile(
                        lowerSegment.getSegmentFiles().getIndexFileName(),
                        segmentFiles.getIndexFileName());
                // Moving segment scarce index file
                segmentFiles.getDirectory().renameFile(
                        lowerSegment.getSegmentFiles().getScarceFileName(),
                        segmentFiles.getScarceFileName());
                // Moving bloom filter file
                segmentFiles.getDirectory().renameFile(
                        lowerSegment.getSegmentFiles().getBloomFilterFileName(),
                        segmentFiles.getBloomFilterFileName());

                deltaCacheController.clear();

                // update segment statistics
                segmentPropertiesManager.setNumberOfKeysInCache(0);
                final SegmentStats stats = lowerSegment
                        .getSegmentPropertiesManager().getSegmentStats();
                segmentPropertiesManager.setNumberOfKeysInIndex(
                        stats.getNumberOfKeysInSegment());
                segmentPropertiesManager.setNumberOfKeysInScarceIndex(
                        stats.getNumberOfKeysInScarceIndex());
                segmentPropertiesManager.flush();
                return new SegmentSplitterResult<>(lowerSegment, minKey.get(),
                        maxKey.get(),
                        SegmentSplitterResult.SegmentSplittingStatus.COMPACTED);
            }
        }

    }

    /*
     * Real number of key is equals or lower than computed bellow. Keys in cache
     * could already be in main index file of it can be keys with tombstone
     * value.
     * 
     * It lead to loading of delta cache into memory.
     * 
     * @return return estimated number of keys in segment
     */
    private long getEstimatedNumberOfKeys() {
        return getStats().getNumberOfKeysInSegment()
                + deltaCacheController.getDeltaCacheSizeWithoutTombstones();
    }

}
