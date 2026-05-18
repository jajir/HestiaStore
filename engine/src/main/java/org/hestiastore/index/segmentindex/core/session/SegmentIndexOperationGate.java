package org.hestiastore.index.segmentindex.core.session;

import java.util.function.Supplier;

/**
 * Gate for foreground segment-index operations that must finish before close
 * can continue.
 *
 * <p>
 * The gate tracks synchronous operations while they execute and exposes a wait
 * point used by close coordination. Lifecycle validation remains the caller's
 * responsibility and should be performed inside the tracked operation.
 * </p>
 */
public interface SegmentIndexOperationGate {

    /**
     * Creates the default operation gate.
     *
     * @return operation gate
     */
    static SegmentIndexOperationGate create() {
        return new SegmentIndexOperationGateImpl();
    }

    /**
     * Executes a task while it is counted as an in-flight foreground
     * operation.
     *
     * @param <T> task result type
     * @param task task to execute
     * @return task result
     */
    <T> T trackOperation(Supplier<T> task);

    /**
     * Waits until currently tracked foreground operations finish.
     */
    void awaitOperationDrain();
}
