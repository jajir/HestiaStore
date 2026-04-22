package org.hestiastore.index.segmentindex.core.routing;

import java.util.function.Supplier;

/**
 * Capability view for tracked foreground index-operation execution and close
 * coordination.
 */
public interface IndexOperationTrackingAccess {

    static IndexOperationTrackingAccess create() {
        return new IndexOperationTracker();
    }

    <T> T runTracked(Supplier<T> task);

    void awaitOperations();
}
