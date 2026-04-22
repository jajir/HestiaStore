package org.hestiastore.index.segmentindex.core.storage;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.slf4j.Logger;

/**
 * Transitions the owning index into the error state when WAL runtime failures
 * indicate broken durability.
 */
final class WalFailureTransitionHandler {

    private final Logger logger;
    private final WalRuntime<?, ?> walRuntime;
    private final Supplier<SegmentIndexState> stateSupplier;
    private final Consumer<RuntimeException> failureHandler;

    WalFailureTransitionHandler(final Logger logger,
            final WalRuntime<?, ?> walRuntime,
            final Supplier<SegmentIndexState> stateSupplier,
            final Consumer<RuntimeException> failureHandler) {
        this.logger = Vldtn.requireNonNull(logger, "logger");
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
        logger.error(
                "event=wal_sync_failure_transition state={} action=transition_to_error reason=wal_sync_failure",
                state, failure);
        failureHandler.accept(failure);
        return failure;
    }
}
