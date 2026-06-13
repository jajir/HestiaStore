package org.hestiastore.index.segmentindex.core.storage;

/**
 * Handles WAL runtime failures that require the owning index to leave the
 * ready state.
 */
public interface WalRuntimeFailureHandler {

    /**
     * Records a WAL runtime failure on the owning index.
     *
     * @param failure WAL runtime failure
     */
    void handleWalRuntimeFailure(RuntimeException failure);
}
