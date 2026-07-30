package org.hestiastore.index.segment;

import java.util.Comparator;
import java.util.NoSuchElementException;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;

/**
 * Restricts an ordered entry iterator to a half-open key range.
 * <p>
 * The lower bound is applied only while positioning the iterator. Subsequent
 * entries are ordered, so iteration only needs to enforce the optional upper
 * bound.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
final class KeyRangeEntryIterator<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    private final EntryIterator<K, V> delegate;
    private final Comparator<K> keyComparator;
    private final K toExclusive;
    private Entry<K, V> next;

    /**
     * Creates a range view over an ordered iterator.
     *
     * @param delegate ordered source iterator
     * @param keyComparator key ordering
     * @param fromInclusive required inclusive lower bound
     * @param toExclusive optional exclusive upper bound
     */
    KeyRangeEntryIterator(final EntryIterator<K, V> delegate,
            final Comparator<K> keyComparator, final K fromInclusive,
            final K toExclusive) {
        this.delegate = Vldtn.requireNonNull(delegate, "delegate");
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.toExclusive = toExclusive;
        seekTo(Vldtn.requireNonNull(fromInclusive, "fromInclusive"));
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        return next != null;
    }

    /** {@inheritDoc} */
    @Override
    public Entry<K, V> next() {
        if (next == null) {
            throw new NoSuchElementException("No next element.");
        }
        final Entry<K, V> result = next;
        advance();
        return result;
    }

    private void seekTo(final K fromInclusive) {
        while (delegate.hasNext()) {
            final Entry<K, V> candidate = delegate.next();
            if (keyComparator.compare(candidate.getKey(), fromInclusive) >= 0) {
                next = belowUpperBound(candidate) ? candidate : null;
                return;
            }
        }
    }

    private void advance() {
        next = null;
        if (delegate.hasNext()) {
            final Entry<K, V> candidate = delegate.next();
            if (belowUpperBound(candidate)) {
                next = candidate;
            }
        }
    }

    private boolean belowUpperBound(final Entry<K, V> candidate) {
        return toExclusive == null
                || keyComparator.compare(candidate.getKey(), toExclusive) < 0;
    }

    /** {@inheritDoc} */
    @Override
    protected void doClose() {
        delegate.close();
        next = null;
    }
}
