package org.hestiastore.index.segmentindex.core;

import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Test-only view over {@link IndexStateCoordinator}.
 */
public final class SegmentIndexStateCoordinator {

    private final IndexStateCoordinator<?, ?> delegate;

    SegmentIndexStateCoordinator(final IndexStateCoordinator<?, ?> delegate) {
        this.delegate = delegate;
    }

    public SegmentIndexState state() {
        return delegate.getState();
    }
}
