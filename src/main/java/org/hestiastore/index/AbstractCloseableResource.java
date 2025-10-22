package org.hestiastore.index;

/**
 * Base class that enforces single-close semantics and exposes the closed flag
 * for diagnostics and tests.
 */
public abstract class AbstractCloseableResource implements CloseableResource {

    private boolean closed;

    @Override
    public final boolean wasClosed() {
        return closed;
    }

    @Override
    public final void close() {
        if (closed) {
            throw new IllegalStateException(
                    getClass().getSimpleName() + " already closed");
        }
        closed = true;
        doClose();
    }

    /**
     * Template method invoked exactly once when the resource is being closed.
     * Subclasses should release their managed resources here.
     */
    protected abstract void doClose();
}
