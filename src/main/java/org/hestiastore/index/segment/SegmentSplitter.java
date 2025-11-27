package org.hestiastore.index.segment;

import org.hestiastore.index.F;
import org.hestiastore.index.Vldtn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Splits a segment into two logical halves by streaming entries in key order.
 * <p>
 * Algorithm: - Create a new “lower” segment and copy the first half of entries
 * into it. - If there are no remaining entries, replace the current segment with
 * the lower segment (compaction outcome). - Otherwise, stream the remaining
 * entries back into the current segment (splitting outcome).
 *
 * The caller supplies a precomputed {@link SegmentSplitterPlan} which carries
 * the target lower size and tracks statistics during the split.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class SegmentSplitter<K, V> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final SegmentImpl<K, V> segment;
    private final VersionController versionController;
    private final SegmentReplacer<K, V> segmentReplacer;

    public SegmentSplitter(final SegmentImpl<K, V> segment,
            final VersionController versionController,
            final SegmentReplacer<K, V> segmentReplacer) {
        this.segment = Vldtn.requireNonNull(segment, "segment");
        this.versionController = Vldtn.requireNonNull(versionController,
                "versionController");
        this.segmentReplacer = Vldtn.requireNonNull(segmentReplacer,
                "segmentReplacer");
    }

    /**
     * Executes a single split operation according to the supplied plan.
     * <p>
     * Pre-conditions: - {@code plan.isSplitFeasible()} is true (otherwise an
     * exception is thrown) - The caller provides a fresh {@code segmentId} for
     * the lower segment
     *
     * Post-conditions: - Returns SPLIT when remaining entries were written back
     * to the current segment; otherwise COMPACTED when the current segment is
     * replaced by the lower segment.
     */
    public SegmentSplitterResult<K, V> split(final SegmentId segmentId,
            final SegmentSplitterPlan<K, V> plan) {
        Vldtn.requireNonNull(segmentId, "segmentId");
        Vldtn.requireNonNull(plan, "plan");
        logger.debug("Splitting of '{}' started", segment.getId());
        segment.resetSegmentIndexSearcher();
        versionController.changeVersion();

        final SegmentSplitContext<K, V> ctx = new SegmentSplitContext<>(segment,
                versionController, plan, segmentId);
        final SegmentSplitPipeline<K, V> pipeline = new SegmentSplitPipeline<>(
                java.util.List.of(new SegmentSplitStepValidateFeasibility<>(),
                        new SegmentSplitStepOpenIterator<>(),
                        new SegmentSplitStepCreateLowerSegment<>(),
                        new SegmentSplitStepFillLowerUntilTarget<>(),
                        new SegmentSplitStepEnsureLowerNotEmpty<>(),
                        new SegmentSplitStepReplaceIfNoRemaining<>(
                                segmentReplacer),
                        new SegmentSplitStepWriteRemainingToCurrent<>()));
        final SegmentSplitterResult<K, V> result = pipeline.run(ctx);

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Splitting of '{}' finished, '{}' was created. Estimated number of keys was '{}'",
                    segment.getId(), result.getSegment().getId(),
                    F.fmt(plan.getEstimatedNumberOfKeys()));
        }
        return result;
    }

}
