package org.hestiastore.index;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class that enforces single-close semantics and exposes the closed flag
 * for diagnostics and tests.
 */
public abstract class AbstractCloseableResource implements CloseableResource {

    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public final boolean wasClosed() {
        return closed.get();
    }

    @Override
    public final void close() {
        if (!closed.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    getClass().getSimpleName() + " already closed");
        }
        doClose();
    }

    /**
     * Template method invoked exactly once when the resource is being closed.
     * Subclasses should release their managed resources here.
     */
    protected abstract void doClose();
}
