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
    private final SegmentFiles<K, V> segmentFiles;
    private final VersionController versionController;
    private final SegmentPropertiesManager segmentPropertiesManager;
    private final SegmentDeltaCacheController<K, V> deltaCacheController;
    private final SegmentFilesRenamer segmentFilesRenamer;
    private final SegmentFactory<K, V> segmentFactory;
    private final SegmentSplitterPolicy<K, V> segmentSplitterPolicy;

    public SegmentSplitter(final Segment<K, V> segment,
            final SegmentFiles<K, V> segmentFiles,
            final VersionController versionController,
            final SegmentPropertiesManager segmentPropertiesManager,
            final SegmentDeltaCacheController<K, V> deltaCacheController,
            final SegmentFilesRenamer segmentFilesRenamer,
            final SegmentFactory<K, V> segmentFactory,
            final SegmentSplitterPolicy<K, V> segmentSplitterPolicy) {
        this.segment = Vldtn.requireNonNull(segment, "segment");
        this.segmentFiles = Vldtn.requireNonNull(segmentFiles, "segmentFiles");
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
        this.segmentPropertiesManager = Vldtn.requireNonNull(
                segmentPropertiesManager, "segmentPropertiesManager");
        this.deltaCacheController = Vldtn.requireNonNull(deltaCacheController,
                "deltaCacheController");
        this.segmentFilesRenamer = Vldtn.requireNonNull(segmentFilesRenamer,
                "segmentFilesRenamer");
        this.segmentFactory = Vldtn.requireNonNull(segmentFactory,
                "segmentFactory");
        this.segmentSplitterPolicy = Vldtn.requireNonNull(segmentSplitterPolicy,
                "segmentSplitterPolicy");
    }

    public boolean shouldBeCompactedBeforeSplitting(
            final long maxNumberOfKeysInSegment) {
        return segmentSplitterPolicy
                .shouldBeCompactedBeforeSplitting(maxNumberOfKeysInSegment);
    }

    public boolean shouldSplit(final long maxNumberOfKeysInSegment) {
        return segmentSplitterPolicy
                .estimateNumberOfKeys() >= maxNumberOfKeysInSegment;
    }

    public long estimatedNumberOfKeys() {
        return segmentSplitterPolicy.estimateNumberOfKeys();
    }

    public SegmentSplitterPlan<K, V> createPlan() {
        return SegmentSplitterPlan.fromPolicy(segmentSplitterPolicy);
    }

    public boolean shouldSplit(final SegmentSplitterPlan<K, V> plan,
            final long maxNumberOfKeysInSegment) {
        Vldtn.requireNonNull(plan, "plan");
        return plan.estimatedNumberOfKeys() >= maxNumberOfKeysInSegment;
    }

    public boolean shouldBeCompactedBeforeSplitting(
            final SegmentSplitterPlan<K, V> plan,
            final long maxNumberOfKeysInSegment) {
        Vldtn.requireNonNull(plan, "plan");
        return segmentSplitterPolicy.shouldBeCompactedBeforeSplitting(
                maxNumberOfKeysInSegment, plan.estimatedNumberOfKeys());
    }

    public long estimatedNumberOfKeys(final SegmentSplitterPlan<K, V> plan) {
        Vldtn.requireNonNull(plan, "plan");
        return plan.estimatedNumberOfKeys();
    }

    public SegmentSplitterResult<K, V> split(final SegmentId segmentId) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        return split(segmentId, createPlan());
    }

    public SegmentSplitterResult<K, V> split(final SegmentId segmentId,
            final SegmentSplitterPlan<K, V> plan) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(plan, "plan");
        logger.debug("Splitting of '{}' started", segmentFiles.getId());
        versionController.changeVersion();

        if (!plan.isSplitFeasible()) {
            throw new IllegalStateException(
                    "Splitting failed. Number of keys is too low.");
        }
        final Segment<K, V> lowerSegment = segmentFactory
                .createSegment(segmentId);

        try (PairIterator<K, V> iterator = segment.openIterator()) {
            writeLowerSegment(iterator, lowerSegment, plan);
            if (!plan.hasLowerKeys()) {
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
            while (plan.lowerCount() < plan.half() && iterator.hasNext()) {
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
                    segmentFiles.getId(), lowerSegment.getId(),
                    F.fmt(plan.estimatedNumberOfKeys()), F.fmt(plan.half()),
                    F.fmt(plan.lowerCount() + plan.higherCount()));
        }
        if (plan.higherCount() == 0) {
            throw new IllegalStateException(String.format(
                    "Splitting failed. Higher segment doesn't contains any data. Estimated number of keys was '%s'",
                    F.fmt(plan.estimatedNumberOfKeys())));
        }
        return new SegmentSplitterResult<>(lowerSegment, plan.minKey(),
                plan.maxKey(),
                SegmentSplitterResult.SegmentSplittingStatus.SPLIT);
    }

    private SegmentSplitterResult<K, V> compactCurrentSegment(
            final Segment<K, V> lowerSegment,
            final SegmentSplitterPlan<K, V> plan) {
        segmentFilesRenamer.renameFiles(lowerSegment.getSegmentFiles(),
                segmentFiles);

        deltaCacheController.clear();

        segmentPropertiesManager.setNumberOfKeysInCache(0);
        final SegmentStats stats = lowerSegment.getSegmentPropertiesManager()
                .getSegmentStats();
        segmentPropertiesManager
                .setNumberOfKeysInIndex(stats.getNumberOfKeysInSegment());
        segmentPropertiesManager.setNumberOfKeysInScarceIndex(
                stats.getNumberOfKeysInScarceIndex());
        segmentPropertiesManager.flush();
        return new SegmentSplitterResult<>(lowerSegment, plan.minKey(),
                plan.maxKey(),
                SegmentSplitterResult.SegmentSplittingStatus.COMPACTED);
    }

}
