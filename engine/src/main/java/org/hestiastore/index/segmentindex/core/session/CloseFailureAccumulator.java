package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.CloseableResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accumulates runtime failures while closing multiple resources and rethrows
 * the first failure with subsequent failures suppressed.
 */
final class CloseFailureAccumulator {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(CloseFailureAccumulator.class);

    private RuntimeException failure;

    void close(final CloseableResource closeableResource) {
        if (closeableResource == null) {
            return;
        }
        try {
            closeableResource.close();
        } catch (final RuntimeException e) {
            LOGGER.error(
                    "Resource close failed while closing index resources. "
                            + "This failure is logged because close continues "
                            + "to release remaining resources; the first "
                            + "failure is rethrown after cleanup and later "
                            + "failures are attached as suppressed.",
                    e);
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
