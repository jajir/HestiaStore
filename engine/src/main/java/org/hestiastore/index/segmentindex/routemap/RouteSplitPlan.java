package org.hestiastore.index.segmentindex.routemap;

import java.util.Optional;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Immutable split describing how one routed segment range is replaced during
 * split publish.
 *
 * @param <K> key type
 */
public final class RouteSplitPlan<K> {

    private final SegmentId replacedSegmentId;
    private final SegmentId lowerSegmentId;
    private final SegmentId upperSegmentId;
    private final K lowerMaxKey;
    private final K upperMaxKey;

    /**
     * Creates an immutable split describing how a split outcome should be
     * published into the route map.
     *
     * @param replacedSegmentId replaced segment id
     * @param lowerSegmentId    newly created lower segment id
     * @param upperSegmentId    newly created upper segment id
     * @param lowerMaxKey       maximum key covered by the lower segment
     * @param upperMaxKey       maximum key covered by the upper segment when it
     *                          is known
     */
    public RouteSplitPlan(final SegmentId replacedSegmentId,
            final SegmentId lowerSegmentId, final SegmentId upperSegmentId,
            final K lowerMaxKey, final K upperMaxKey) {
        this.replacedSegmentId = Vldtn.requireNonNull(replacedSegmentId,
                "replacedSegmentId");
        this.lowerSegmentId = Vldtn.requireNonNull(lowerSegmentId,
                "lowerSegmentId");
        this.upperSegmentId = Vldtn.requireNonNull(upperSegmentId,
                "upperSegmentId");
        this.lowerMaxKey = Vldtn.requireNonNull(lowerMaxKey, "lowerMaxKey");
        this.upperMaxKey = upperMaxKey;
    }

    /**
     * @return id of the segment being replaced
     */
    public SegmentId getReplacedSegmentId() {
        return replacedSegmentId;
    }

    /**
     * @return id of the lower segment produced by split
     */
    public SegmentId getLowerSegmentId() {
        return lowerSegmentId;
    }

    /**
     * @return id of the upper segment produced by split
     */
    public SegmentId getUpperSegmentId() {
        return upperSegmentId;
    }

    /**
     * @return maximum key covered by the lower segment
     */
    public K getLowerMaxKey() {
        return lowerMaxKey;
    }

    /**
     * @return maximum key covered by the upper segment when known
     */
    public Optional<K> getUpperMaxKey() {
        return Optional.ofNullable(upperMaxKey);
    }
}
