package org.hestiastore.index.segmentindex.core.split;

import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Exposes the host runtime lifecycle view required by the split subsystem.
 */
interface SplitRuntimeLifecycle {

    /**
     * @return current host runtime state
     */
    SegmentIndexState currentState();

    /**
     * @return {@code true} when the runtime should reject new background split
     *         work
     */
    default boolean rejectsBackgroundWork() {
        final SegmentIndexState state = currentState();
        return state == SegmentIndexState.CLOSING
                || state == SegmentIndexState.CLOSED
                || state == SegmentIndexState.ERROR;
    }

    /**
     * Adapts a generic state supplier to the split lifecycle contract.
     *
     * @param stateSupplier host runtime state supplier
     * @return split lifecycle contract
     */
    static SplitRuntimeLifecycle from(
            final Supplier<SegmentIndexState> stateSupplier) {
        final Supplier<SegmentIndexState> validatedStateSupplier = Vldtn
                .requireNonNull(stateSupplier, "stateSupplier");
        return new SplitRuntimeLifecycle() {
            @Override
            public SegmentIndexState currentState() {
                return validatedStateSupplier.get();
            }
        };
    }
}
