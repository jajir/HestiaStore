package org.hestiastore.index;

/**
 * EntryWriter wrapper that enforces single-use semantics.
 */
public final class GuardedEntryWriter<K, V> extends AbstractCloseableResource
        implements EntryWriter<K, V> {

    private final EntryWriter<K, V> delegate;

    public GuardedEntryWriter(final EntryWriter<K, V> delegate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    @Override
    public void write(final Entry<K, V> entry) {
        ensureOpen();
        delegate.write(entry);
    }

    private void ensureOpen() {
        if (wasClosed()) {
            throw new IllegalStateException("Writer already closed");
        }
    }

    @Override
    protected void doClose() {
        delegate.close();
    }
}
