package org.hestiastore.index.segmentindex.core.split;

import org.hestiastore.index.Vldtn;

/**
 * Late-binding router used to connect internal split-runtime event producers
 * and consumers without exposing generic callbacks in public constructors.
 */
final class SplitRuntimeEventRouter implements SplitRuntimeEvents {

    private volatile SplitRuntimeEvents delegate = SplitRuntimeEvents.noOp();

    void attach(final SplitRuntimeEvents delegate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    @Override
    public void onSplitApplied() {
        delegate.onSplitApplied();
    }
}
