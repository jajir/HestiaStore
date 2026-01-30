package org.hestiastore.index.segmentregistry;

/**
 * Handle for a registry FREEZE window. Closing the handle releases the freeze.
 */
public interface SegmentRegistryFreeze extends AutoCloseable {

    /**
     * Releases the registry FREEZE state.
     */
    @Override
    void close();
}
