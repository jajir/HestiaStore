package org.hestiastore.index.segmentindex.core.split;

/**
 * Reports fatal split runtime failures to the owning runtime.
 */
@FunctionalInterface
public interface SplitFailureReporter {

    /**
     * Reports a fatal split-runtime failure.
     *
     * @param failure fatal failure
     */
    void reportFailure(RuntimeException failure);

    /**
     * @return no-op failure reporter for tests
     */
    static SplitFailureReporter noOp() {
        return failure -> {
        };
    }
}
