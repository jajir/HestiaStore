package org.hestiastore.index;

/**
 * Basic contract for closeable resources used across the index. Unlike
 * {@link java.io.Closeable}, this interface does not declare
 * {@link java.io.IOException} and exposes a simple lifecycle probe.
 */
public interface CloseableResource extends AutoCloseable {

    /**
     * @return {@code true} once {@link #close()} has been invoked
     */
    boolean wasClosed();

    /**
     * Releases the resource. Implementations must throw
     * {@link IllegalStateException} if the resource is already closed.
     */
    @Override
    void close();
}
