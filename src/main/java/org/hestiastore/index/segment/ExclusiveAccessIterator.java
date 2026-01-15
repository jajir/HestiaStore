package org.hestiastore.index.segment;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;

/**
 * Iterator wrapper used for {@link SegmentIteratorIsolation#FULL_ISOLATION}.
 * It keeps the owning segment in {@code FREEZE} and returns it to {@code READY}
 * when closed, guaranteeing exclusive access for the duration of iteration.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class ExclusiveAccessIterator<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    private final EntryIterator<K, V> delegate;
    private final SegmentConcurrencyGate gate;

    /**
     * Creates an iterator that holds the segment in freeze state until closed.
     *
     * @param delegate underlying iterator to read from
     * @param gate concurrency gate to release on close
     */
    ExclusiveAccessIterator(final EntryIterator<K, V> delegate,
            final SegmentConcurrencyGate gate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.gate = Vldtn.requireNonNull(gate, "gate");
    }

    /**
     * Returns whether the underlying iterator has another entry.
     *
     * @return true when another entry is available
     */
    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    /**
     * Returns the next entry from the underlying iterator.
     *
     * @return next entry
     */
    @Override
    public Entry<K, V> next() {
        return delegate.next();
    }

    /**
     * Closes the delegate and releases the exclusive freeze state.
     */
    @Override
    protected void doClose() {
        try {
            delegate.close();
        } finally {
            gate.finishFreezeToReady();
        }
    }
}
