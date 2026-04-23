package org.hestiastore.index.segmentindex.core.split;

import java.util.function.Consumer;

import org.hestiastore.index.Vldtn;

/**
 * Reports fatal split runtime failures to the owning runtime.
 */
@FunctionalInterface
interface SplitFailureReporter {

    /**
     * Reports a fatal background split failure.
     *
     * @param failure fatal failure
     */
    void reportFailure(RuntimeException failure);

    /**
     * Adapts a generic consumer to the split failure contract.
     *
     * @param failureHandler fatal failure consumer
     * @return split failure reporter
     */
    static SplitFailureReporter from(
            final Consumer<RuntimeException> failureHandler) {
        final Consumer<RuntimeException> validatedFailureHandler = Vldtn
                .requireNonNull(failureHandler, "failureHandler");
        return new SplitFailureReporter() {
            @Override
            public void reportFailure(final RuntimeException failure) {
                validatedFailureHandler.accept(failure);
            }
        };
    }

    /**
     * @return no-op failure reporter for tests
     */
    static SplitFailureReporter noOp() {
        return failure -> {
        };
    }
}
