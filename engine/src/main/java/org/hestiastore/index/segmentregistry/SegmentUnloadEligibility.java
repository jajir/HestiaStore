package org.hestiastore.index.segmentregistry;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;

/**
 * Decides whether a loaded segment can enter registry cache unload.
 */
final class SegmentUnloadEligibility {

    private final SegmentRegistryStateMachine gate;

    SegmentUnloadEligibility(final SegmentRegistryStateMachine gate) {
        this.gate = Vldtn.requireNonNull(gate, "gate");
    }

    boolean canUnload(final Segment<?, ?> segment) {
        return segment != null && (segment.getState() == SegmentState.CLOSED
                || isReadySegmentEvictable(segment));
    }

    boolean canForceUnload(final Segment<?, ?> segment) {
        return segment != null && (segment.getState() == SegmentState.CLOSED
                || isReadySegmentWithId(segment));
    }

    private boolean isReadySegmentEvictable(final Segment<?, ?> segment) {
        final boolean closing = gate.getState() != SegmentRegistryState.READY;
        return isReadySegmentWithId(segment)
                && (closing || segment.getNumberOfKeysInWriteCache() == 0);
    }

    private boolean isReadySegmentWithId(final Segment<?, ?> segment) {
        final SegmentId segmentId = segment.getId();
        return segmentId != null && segment.getState() == SegmentState.READY;
    }
}
