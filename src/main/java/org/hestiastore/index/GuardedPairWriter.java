package org.hestiastore.index;

/**
 * PairWriter wrapper that enforces single-use semantics.
 */
public final class GuardedPairWriter<K, V> implements PairWriter<K, V> {

    private final PairWriter<K, V> delegate;
    private boolean closed;

    public GuardedPairWriter(final PairWriter<K, V> delegate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
    }

    @Override
    public void write(final Pair<K, V> pair) {
        ensureOpen();
        delegate.write(pair);
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            delegate.close();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Writer already closed");
        }
    }
}
