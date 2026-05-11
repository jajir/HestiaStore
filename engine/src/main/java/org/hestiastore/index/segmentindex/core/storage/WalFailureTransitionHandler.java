package org.hestiastore.index.segmentindex.core.storage;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexState;
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
    private final Supplier<SegmentIndexState> stateSupplier;
    private final Consumer<RuntimeException> failureHandler;

    WalFailureTransitionHandler(
            final WalRuntime<?, ?> walRuntime,
            final Supplier<SegmentIndexState> stateSupplier,
            final Consumer<RuntimeException> failureHandler) {
        this.walRuntime = Vldtn.requireNonNull(walRuntime, "walRuntime");
        this.stateSupplier = Vldtn.requireNonNull(stateSupplier,
                "stateSupplier");
        this.failureHandler = Vldtn.requireNonNull(failureHandler,
                "failureHandler");
    }

    RuntimeException propagate(final RuntimeException failure) {
        if (!walRuntime.isEnabled() || !walRuntime.hasSyncFailure()) {
            return failure;
        }
        final SegmentIndexState state = stateSupplier.get();
        if (state == SegmentIndexState.CLOSED
                || state == SegmentIndexState.ERROR) {
            return failure;
        }
        LOGGER.error(
                "event=wal_sync_failure_transition state={} action=transition_to_error reason=wal_sync_failure",
                state, failure);
        failureHandler.accept(failure);
        return failure;
    }
}
