package org.hestiastore.index.segmentindex.core.split;

/**
 * Collects best-effort prepared-segment cleanup failures.
 */
final class PreparedSegmentCleanupErrors {

    private RuntimeException failure;

    void add(final RuntimeException nextFailure) {
        if (failure == null) {
            failure = nextFailure;
            return;
        }
        failure.addSuppressed(nextFailure);
    }

    void throwIfAny() {
        if (failure != null) {
            throw failure;
        }
    }
}
