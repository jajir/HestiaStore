package org.hestiastore.index;

/**
 * PairWriter wrapper that enforces single-use semantics.
 */
public final class GuardedPairWriter<K, V> extends AbstractCloseableResource
        implements PairWriter<K, V> {

    private final PairWriter<K, V> delegate;

    public GuardedPairWriter(final PairWriter<K, V> delegate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    @Override
    public void write(final Pair<K, V> pair) {
        ensureOpen();
        delegate.write(pair);
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
