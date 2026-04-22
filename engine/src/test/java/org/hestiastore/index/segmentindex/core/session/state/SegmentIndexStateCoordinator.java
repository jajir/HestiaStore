package org.hestiastore.index.segmentindex.core.session.state;

import org.hestiastore.index.segmentindex.SegmentIndexState;

/**
 * Test-only view over {@link IndexStateCoordinator}.
 */
public final class SegmentIndexStateCoordinator {

    private final IndexStateCoordinator<?, ?> delegate;

    public SegmentIndexStateCoordinator(
            final IndexStateCoordinator<?, ?> delegate) {
        this.delegate = delegate;
    }

    public SegmentIndexState state() {
        return delegate.getState();
    }

    public void failWithError(final Throwable failure) {
        delegate.failWithError(failure);
    }

    public void beginClose() {
        delegate.beginClose();
    }

    public void completeCloseStateTransition() {
        delegate.completeCloseStateTransition();
    }
}
