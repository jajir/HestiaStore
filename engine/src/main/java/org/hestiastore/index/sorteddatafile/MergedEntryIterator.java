package org.hestiastore.index.sorteddatafile;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.hestiastore.index.Vldtn;

/**
 * Merges multiple sorted iterators into a single sorted iterator.
 *
 * <p>
 * When duplicate keys are encountered across iterators, values are combined
 * using the provided {@link Merger}.
 * </p>
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class MergedEntryIterator<K, V>
        extends AbstractCloseableResource implements EntryIteratorWithCurrent<K, V> {

    private final List<EntryIteratorWithCurrent<K, V>> iterators;
    private final Comparator<K> keyComparator;
    private final Merger<K, V> merger;
    private Entry<K, V> current;
    private Entry<K, V> next;

    /**
     * Creates a merged iterator over the provided iterators.
     *
     * @param iterators sorted iterators with current entry support
     * @param keyComparator comparator used to order keys
     * @param merger value merger for duplicate keys
     */
    MergedEntryIterator(final List<EntryIteratorWithCurrent<K, V>> iterators,
            final Comparator<K> keyComparator, final Merger<K, V> merger) {
        Vldtn.requireNonNull(iterators, "iterators");
        this.iterators = new ArrayList<>();
        iterators.forEach(iterator -> {
            if (iterator.hasNext()) {
                iterator.next();
                this.iterators.add(iterator);
            } else {
                iterator.close();
            }
        });
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.merger = Vldtn.requireNonNull(merger, "merger");
        next = moveToNextEntry();
    }

    /**
     * Moves to the next merged entry based on current iterator positions.
     *
     * @return next merged entry or null if exhausted
     */
    private Entry<K, V> moveToNextEntry() {
        final Optional<EntryIteratorWithCurrent<K, V>> oLowestIter = findIteratorWithLowestKey();
        if (!oLowestIter.isPresent()) {
            return null;
        }
        final EntryIteratorWithCurrent<K, V> lowestIter = oLowestIter.get();
        if (lowestIter.getCurrent().isPresent()) {
            final K lowestKey = oLowestIter.get().getCurrent().get().getKey();
            return moveNextIteratorsWithKey(lowestKey);
        } else {
            throw new IllegalStateException(
                    "lowestIter.getCurrent() must not be empty");
        }
    }

    /**
     * Finds the iterator whose current entry has the lowest key.
     *
     * @return iterator with the lowest key, or empty when none are available
     */
    public Optional<EntryIteratorWithCurrent<K, V>> findIteratorWithLowestKey() {
        final Comparator<EntryIteratorWithCurrent<K, V>> comparator = new EntryIteratorWithCurrentComparator<>(
                keyComparator);
        final List<EntryIteratorWithCurrent<K, V>> toRemove = new ArrayList<>();
        EntryIteratorWithCurrent<K, V> lowest = null;

        for (final EntryIteratorWithCurrent<K, V> iterator : iterators) {
            if (iterator.getCurrent().isPresent()) {
                if (lowest == null) {
                    lowest = iterator;
                } else {
                    if (comparator.compare(iterator, lowest) < 0) {
                        lowest = iterator;
                    }
                }
            } else {
                toRemove.add(iterator);
            }
        }
        iterators.removeAll(toRemove);
        return Optional.ofNullable(lowest);
    }

    /**
     * Advances all iterators that currently point to the given key and merges
     * their values.
     *
     * @param key required key to merge
     * @return merged entry for the key
     */
    private Entry<K, V> moveNextIteratorsWithKey(final K key) {
        Vldtn.requireNonNull(key, "key");
        final List<EntryIteratorWithCurrent<K, V>> toRemove = new ArrayList<>();
        V out = null;
        for (final EntryIteratorWithCurrent<K, V> iter : iterators) {
            if (iter.getCurrent().isPresent()) {
                final Entry<K, V> entry = iter.getCurrent().get();
                final K k = entry.getKey();
                if (keyComparator.compare(k, key) == 0) {
                    if (out == null) {
                        out = entry.getValue();
                    } else {
                        out = merger.merge(key, out, entry.getValue());
                    }
                    moveIteratorToNextEntry(iter, toRemove);
                }
            }
        }
        iterators.removeAll(toRemove);
        Vldtn.requireNonNull(out, "outKey");
        return new Entry<K, V>(key, out);
    }

    /**
     * Advances the given iterator or removes it when exhausted.
     *
     * @param iterator iterator to advance
     * @param toRemove list collecting exhausted iterators
     */
    private void moveIteratorToNextEntry(
            final EntryIteratorWithCurrent<K, V> iterator,
            final List<EntryIteratorWithCurrent<K, V>> toRemove) {
        if (iterator.hasNext()) {
            iterator.next();
        } else {
            iterator.close();
            toRemove.add(iterator);
        }
    }

    /**
     * Returns true if another merged entry is available.
     *
     * @return true when another entry can be read
     */
    @Override
    public boolean hasNext() {
        return next != null;
    }

    /**
     * Returns the next merged entry.
     *
     * @return next entry in sorted order
     */
    @Override
    public Entry<K, V> next() {
        if (next == null) {
            throw new NoSuchElementException();
        } else {
            current = next;
            next = moveToNextEntry();
            return current;
        }
    }

    /**
     * Closes all remaining iterators and clears internal state.
     */
    @Override
    protected void doClose() {
        for (final EntryIteratorWithCurrent<K, V> iterator : iterators) {
            if (iterator.hasNext()) {
                iterator.close();
            }
        }
        iterators.clear();
        current = null;
        next = null;
    }

    /**
     * Returns the most recently produced entry, if any.
     *
     * @return current entry, or empty when none has been produced
     */
    @Override
    public Optional<Entry<K, V>> getCurrent() {
        return Optional.ofNullable(current);
    }

}
