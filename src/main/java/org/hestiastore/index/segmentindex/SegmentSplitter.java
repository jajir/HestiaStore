package org.hestiastore.index.segmentindex;

import java.util.List;

import org.hestiastore.index.F;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
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
    private final Segment<K, V> segment;
    private final SegmentWriterTxFactory<K, V> writerTxFactory;

    public SegmentSplitter(final Segment<K, V> segment,
            final SegmentWriterTxFactory<K, V> writerTxFactory) {
        this.segment = Vldtn.requireNonNull(segment, "segment");
        this.writerTxFactory = Vldtn.requireNonNull(writerTxFactory,
                "writerTxFactory");
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
        segment.invalidateIterators();

        final SegmentSplitContext<K, V> ctx = new SegmentSplitContext<>(segment,
                plan, segmentId, writerTxFactory);
        final SegmentSplitPipeline<K, V> pipeline = new SegmentSplitPipeline<>(
                List.of(new SegmentSplitStepValidateFeasibility<>(),
                        new SegmentSplitStepOpenIterator<>(),
                        new SegmentSplitStepCreateLowerSegment<>(),
                        new SegmentSplitStepFillLowerUntilTarget<>(),
                        new SegmentSplitStepEnsureLowerNotEmpty<>(),
                        new SegmentSplitStepReplaceIfNoRemaining<>(),
                        new SegmentSplitStepWriteRemainingToCurrent<>()));
        final SegmentSplitterResult<K, V> result = pipeline.run(ctx);

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Splitting of '{}' finished, '{}' was created. Estimated number of keys was '{}'",
                    segment.getId(), result.getSegmentId(),
                    F.fmt(plan.getEstimatedNumberOfKeys()));
        }
        return result;
    }

}
