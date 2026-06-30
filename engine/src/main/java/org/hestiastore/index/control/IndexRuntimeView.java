package org.hestiastore.index.control;

import org.hestiastore.index.control.model.IndexRuntimeSnapshot;

/**
 * View for runtime parameters that are changing constantly.
 */
public interface IndexRuntimeView {

    /**
     * Returns immutable runtime metrics/state snapshot.
     *
     * @return runtime snapshot
     */
    IndexRuntimeSnapshot snapshot();
}
