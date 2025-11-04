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

public final class MergedEntryIterator<K, V>
        extends AbstractCloseableResource implements EntryIteratorWithCurrent<K, V> {

    private final List<EntryIteratorWithCurrent<K, V>> iterators;
    private final Comparator<K> keyComparator;
    private final Merger<K, V> merger;
    private Entry<K, V> current;
    private Entry<K, V> next;

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

    @Override
    public boolean hasNext() {
        return next != null;
    }

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

    @Override
    public Optional<Entry<K, V>> getCurrent() {
        return Optional.ofNullable(current);
    }

}
