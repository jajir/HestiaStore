package org.hestiastore.index.segment;

import org.hestiastore.index.F;
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
    private final VersionController versionController;
    private final SegmentFactory<K, V> segmentFactory;
    private final SegmentReplacer<K, V> splitApplier;

    public SegmentSplitter(final Segment<K, V> segment,
            final VersionController versionController,
            final SegmentFactory<K, V> segmentFactory,
            final SegmentReplacer<K, V> splitApplier) {
        this.segment = Vldtn.requireNonNull(segment, "segment");
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
        this.segmentFactory = Vldtn.requireNonNull(segmentFactory,
                "segmentFactory");
        this.splitApplier = Vldtn.requireNonNull(splitApplier, "splitApplier");
    }

    public SegmentSplitterResult<K, V> split(final SegmentId segmentId,
            final SegmentSplitterPlan<K, V> plan) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(plan, "plan");
        logger.debug("Splitting of '{}' started", segment.getId());
        versionController.changeVersion();

        if (!plan.isSplitFeasible()) {
            throw new IllegalStateException(
                    "Splitting failed. Number of keys is too low.");
        }
        final Segment<K, V> lowerSegment = segmentFactory
                .createSegment(segmentId);

        try (PairIterator<K, V> iterator = segment.openIterator()) {
            writeLowerSegment(iterator, lowerSegment, plan);
            if (plan.isLowerSegmentEmpty()) {
                throw new IllegalStateException(
                        "Splitting failed. Lower segment doesn't contains any data");
            }

            if (iterator.hasNext()) {
                return performSegmentSplit(iterator, lowerSegment, plan);
            }
            return compactCurrentSegment(lowerSegment, plan);
        }

    }

    private void writeLowerSegment(final PairIterator<K, V> iterator,
            final Segment<K, V> lowerSegment,
            final SegmentSplitterPlan<K, V> plan) {
        final WriteTransaction<K, V> lowerSegmentWriteTx = lowerSegment
                .openFullWriteTx();
        try (PairWriter<K, V> writer = lowerSegmentWriteTx.openWriter()) {
            while (plan.getLowerCount() < plan.getHalf() && iterator.hasNext()) {
                final Pair<K, V> pair = iterator.next();
                plan.recordLower(pair);
                writer.write(pair);
            }
        }
        lowerSegmentWriteTx.commit();
    }

    private SegmentSplitterResult<K, V> performSegmentSplit(
            final PairIterator<K, V> iterator, final Segment<K, V> lowerSegment,
            final SegmentSplitterPlan<K, V> plan) {
        final WriteTransaction<K, V> segmentWriteTx = segment.openFullWriteTx();
        try (PairWriter<K, V> writer = segmentWriteTx.openWriter()) {
            while (iterator.hasNext()) {
                final Pair<K, V> pair = iterator.next();
                writer.write(pair);
                plan.recordUpper();
            }
        }
        segmentWriteTx.commit();

        if (logger.isDebugEnabled()) {
            logger.debug("Splitting of '{}' finished, '{}' was created. "
                    + "Estimated number of keys was '{}', "
                    + "half key was '{}' and real number of keys was '{}'.",
                    segment.getId(), lowerSegment.getId(),
                    F.fmt(plan.getEstimatedNumberOfKeys()), F.fmt(plan.getHalf()),
                    F.fmt(plan.getLowerCount() + plan.getHigherCount()));
        }
        if (plan.getHigherCount() == 0) {
            throw new IllegalStateException(String.format(
                    "Splitting failed. Higher segment doesn't contains any data. Estimated number of keys was '%s'",
                    F.fmt(plan.getEstimatedNumberOfKeys())));
        }
        return new SegmentSplitterResult<>(lowerSegment, plan.getMinKey(),
                plan.getMaxKey(),
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
    }

    private SegmentSplitterResult<K, V> compactCurrentSegment(
            final Segment<K, V> lowerSegment,
            final SegmentSplitterPlan<K, V> plan) {
        splitApplier.replaceWithLower(lowerSegment);
        return new SegmentSplitterResult<>(lowerSegment, plan.getMinKey(),
                plan.getMaxKey(),
                SegmentSplitterResult.SegmentSplittingStatus.COMPACTED);
    }

}
