package org.hestiastore.index.segmentindex.core.storage;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateView;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transitions the owning index into the error state when WAL runtime failures
 * indicate broken durability.
 */
final class WalFailureTransitionHandler {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(WalFailureTransitionHandler.class);

    private final WalRuntime<?, ?> walRuntime;
    private final SegmentIndexStateView stateView;
    private final WalRuntimeFailureHandler failureHandler;

    WalFailureTransitionHandler(
            final WalRuntime<?, ?> walRuntime,
            final SegmentIndexStateView stateView,
            final WalRuntimeFailureHandler failureHandler) {
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
        this.stateView = Vldtn.requireNonNull(stateView, "stateView");
        this.failureHandler = Vldtn.requireNonNull(failureHandler,
                "failureHandler");
    }

    RuntimeException propagate(final RuntimeException failure) {
        if (!walRuntime.hasSyncFailure()) {
            return failure;
        }
        final SegmentIndexState state = stateView.currentState();
        if (state == SegmentIndexState.CLOSED
                || state == SegmentIndexState.ERROR) {
            return failure;
        }
        LOGGER.error(
                "event=wal_sync_failure_transition state={} action=transition_to_error reason=wal_sync_failure",
                state, failure);
        failureHandler.handleWalRuntimeFailure(failure);
        return failure;
    }
}
