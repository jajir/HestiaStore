package org.hestiastore.index;

/**
 * Utility base that enforces single-open/single-commit semantics for a
 * closeable resource used by higher-level write transactions. Implementations
 * provide the concrete resource through {@link #doOpen()}; this class ensures
 * {@link #open()} can be called only once and {@link #commit()} is invoked at
 * most once after a successful open.
 *
 * @param <T> the closeable resource type managed by the transaction
 */
public abstract class GuardedWriteTransaction<T extends CloseableResource>
        implements Commitable {

    private boolean opened;
    private boolean committed;
    private T resource;

    public final T open() {
        if (opened) {
            throw new IllegalStateException("Resource already opened");
        }
        opened = true;
        resource = Vldtn.requireNonNull(doOpen(), "resource");
        return resource;
    }

    /**
     * Commits the transaction once the resource has been opened. Repeated calls
     * result in an {@link IllegalStateException}.
     */
    @Override
    public final void commit() {
        if (!opened) {
            throw new IllegalStateException("Resource has not been opened");
        }
        if (committed) {
            throw new IllegalStateException("Transaction already committed");
        }
        if (resource == null) {
            throw new IllegalStateException(
                    "Resource is null. Make sure that transaction was opened properly.");
        }
        if (!resource.wasClosed()) {
            throw new IllegalStateException(
                    "Resource must be closed before commit");
        }
        committed = true;
        doCommit(resource);
    }

    /**
     * Implementations create the concrete resource. Called exactly once from
     * {@link #open()}.
     */
    protected abstract T doOpen();

    /**
     * Implementations perform commit logic. Called exactly once from
     * {@link #commit()}.
     */
    protected abstract void doCommit(T resource);
}
