package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.CloseableResource;

/**
 * Accumulates runtime failures while closing multiple resources and rethrows
 * the first failure with subsequent failures suppressed.
 */
final class CloseFailureAccumulator {

    private RuntimeException failure;

    void close(final CloseableResource closeableResource) {
        if (closeableResource == null) {
            return;
        }
        try {
            closeableResource.close();
        } catch (final RuntimeException e) {
            record(e);
        }
    }

    void closeIfResource(final Object candidate) {
        if (candidate instanceof CloseableResource closeableResource) {
            close(closeableResource);
        }
    }

    void rethrowIfPresent() {
        if (failure != null) {
            throw failure;
        }
    }

    private void record(final RuntimeException nextFailure) {
        if (failure == null) {
            failure = nextFailure;
            return;
        }
        failure.addSuppressed(nextFailure);
    }
}
