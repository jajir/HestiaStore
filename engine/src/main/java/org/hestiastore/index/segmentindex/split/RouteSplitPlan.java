package org.hestiastore.index.segmentindex.split;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Immutable plan describing how one routed segment range is replaced during
 * split publish.
 *
 * @param <K> key type
 */
public final class RouteSplitPlan<K> {

    /**
     * Shape of the route-map update produced by split materialization.
     */
    public enum SplitMode {
        COMPACTED,
        SPLIT
    }

    private final SegmentId replacedSegmentId;
    private final SegmentId lowerSegmentId;
    private final SegmentId upperSegmentId;
    private final K lowerMinKey;
    private final K lowerMaxKey;
    private final SplitMode splitMode;

    /**
     * Creates an immutable plan describing how split/compaction outcome should
     * be published into the route map.
     *
     * @param replacedSegmentId replaced segment id
     * @param lowerSegmentId newly created lower segment id
     * @param upperSegmentId new upper segment id; required for SPLIT mode
     * @param lowerMinKey minimum key covered by the lower segment
     * @param lowerMaxKey maximum key covered by the lower segment
     * @param splitMode split outcome mode
     */
    public RouteSplitPlan(final SegmentId replacedSegmentId,
            final SegmentId lowerSegmentId,
            final SegmentId upperSegmentId, final K lowerMinKey,
            final K lowerMaxKey, final SplitMode splitMode) {
        this.replacedSegmentId = Vldtn.requireNonNull(replacedSegmentId,
                "replacedSegmentId");
        this.lowerSegmentId = Vldtn.requireNonNull(lowerSegmentId,
                "lowerSegmentId");
        this.splitMode = Vldtn.requireNonNull(splitMode, "splitMode");
        if (splitMode == SplitMode.SPLIT) {
            this.upperSegmentId = Vldtn.requireNonNull(upperSegmentId,
                    "upperSegmentId");
        } else {
            this.upperSegmentId = upperSegmentId;
        }
        this.lowerMinKey = Vldtn.requireNonNull(lowerMinKey, "lowerMinKey");
        this.lowerMaxKey = Vldtn.requireNonNull(lowerMaxKey, "lowerMaxKey");
    }

    /**
     * @return id of the segment being replaced
     */
    public SegmentId getReplacedSegmentId() {
        return replacedSegmentId;
    }

    /**
     * @return id of the lower segment produced by split/compaction
     */
    public SegmentId getLowerSegmentId() {
        return lowerSegmentId;
    }

    /**
     * @return optional upper segment id (present for SPLIT, absent for COMPACTED)
     */
    public Optional<SegmentId> getUpperSegmentId() {
        return Optional.ofNullable(upperSegmentId);
    }

    /**
     * @return minimum key covered by the lower segment
     */
    K getLowerMinKey() {
        return lowerMinKey;
    }

    /**
     * @return maximum key covered by the lower segment
     */
    public K getLowerMaxKey() {
        return lowerMaxKey;
    }

    /**
     * @return split outcome status
     */
    public SplitMode getSplitMode() {
        return splitMode;
    }

    /**
     * @return {@code true} when split produced both lower and upper child
     *         segments
     */
    public boolean isSplit() {
        return splitMode == SplitMode.SPLIT;
    }
}
