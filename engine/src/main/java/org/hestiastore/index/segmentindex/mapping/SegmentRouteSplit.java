package org.hestiastore.index.segmentindex.mapping;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.SegmentId;

/**
 * Immutable split describing how one routed segment range is replaced during
 * split publish.
 *
 * @param <K> key type
 */
public final class SegmentRouteSplit<K> {

    private final SegmentId replacedSegmentId;
    private final SegmentId lowerSegmentId;
    private final SegmentId upperSegmentId;
    private final K lowerMaxKey;

    /**
     * Creates an immutable split describing how a split outcome should be
     * published into the route map.
     *
     * @param replacedSegmentId replaced segment id
     * @param lowerSegmentId    newly created lower segment id
     * @param upperSegmentId    newly created upper segment id
     * @param lowerMaxKey       maximum key covered by the lower segment
     */
    public SegmentRouteSplit(final SegmentId replacedSegmentId,
            final SegmentId lowerSegmentId, final SegmentId upperSegmentId,
            final K lowerMaxKey) {
        this.replacedSegmentId = Vldtn.requireNonNull(replacedSegmentId,
                "replacedSegmentId");
        this.lowerSegmentId = Vldtn.requireNonNull(lowerSegmentId,
                "lowerSegmentId");
        this.upperSegmentId = Vldtn.requireNonNull(upperSegmentId,
                "upperSegmentId");
        this.lowerMaxKey = Vldtn.requireNonNull(lowerMaxKey, "lowerMaxKey");
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
}
