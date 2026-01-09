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

    ExclusiveAccessIterator(final EntryIterator<K, V> delegate,
            final SegmentConcurrencyGate gate) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.gate = Vldtn.requireNonNull(gate, "gate");
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public Entry<K, V> next() {
        return delegate.next();
    }

    @Override
    protected void doClose() {
        try {
            delegate.close();
        } finally {
            gate.finishFreezeToReady();
        }
    }
}
