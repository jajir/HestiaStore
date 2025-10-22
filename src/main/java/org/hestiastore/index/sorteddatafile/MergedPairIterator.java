package org.hestiastore.index.sorteddatafile;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIteratorWithCurrent;
import org.hestiastore.index.Vldtn;

public final class MergedPairIterator<K, V>
        extends AbstractCloseableResource implements PairIteratorWithCurrent<K, V> {

    private final List<PairIteratorWithCurrent<K, V>> iterators;
    private final Comparator<K> keyComparator;
    private final Merger<K, V> merger;
    private Pair<K, V> current;
    private Pair<K, V> next;

    MergedPairIterator(final List<PairIteratorWithCurrent<K, V>> iterators,
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
        next = moveToNextPair();
    }

    private Pair<K, V> moveToNextPair() {
        final Optional<PairIteratorWithCurrent<K, V>> oLowestIter = findIteratorWithLowestKey();
        if (!oLowestIter.isPresent()) {
            return null;
        }
        final PairIteratorWithCurrent<K, V> lowestIter = oLowestIter.get();
        if (lowestIter.getCurrent().isPresent()) {
            final K lowestKey = oLowestIter.get().getCurrent().get().getKey();
            return moveNextIteratorsWithKey(lowestKey);
        } else {
            throw new IllegalStateException(
                    "lowestIter.getCurrent() must not be empty");
        }
    }

    public Optional<PairIteratorWithCurrent<K, V>> findIteratorWithLowestKey() {
        final Comparator<PairIteratorWithCurrent<K, V>> comparator = new PairIteratorWithCurrentComparator<>(
                keyComparator);
        final List<PairIteratorWithCurrent<K, V>> toRemove = new ArrayList<>();
        PairIteratorWithCurrent<K, V> lowest = null;

        for (final PairIteratorWithCurrent<K, V> iterator : iterators) {
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

    private Pair<K, V> moveNextIteratorsWithKey(final K key) {
        Vldtn.requireNonNull(key, "key");
        final List<PairIteratorWithCurrent<K, V>> toRemove = new ArrayList<>();
        V out = null;
        for (final PairIteratorWithCurrent<K, V> iter : iterators) {
            if (iter.getCurrent().isPresent()) {
                final Pair<K, V> pair = iter.getCurrent().get();
                final K k = pair.getKey();
                if (keyComparator.compare(k, key) == 0) {
                    if (out == null) {
                        out = pair.getValue();
                    } else {
                        out = merger.merge(key, out, pair.getValue());
                    }
                    moveIteratorToNextPair(iter, toRemove);
                }
            }
        }
        iterators.removeAll(toRemove);
        Vldtn.requireNonNull(out, "outKey");
        return new Pair<K, V>(key, out);
    }

    private void moveIteratorToNextPair(
            final PairIteratorWithCurrent<K, V> iterator,
            final List<PairIteratorWithCurrent<K, V>> toRemove) {
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
    public Pair<K, V> next() {
        if (next == null) {
            throw new NoSuchElementException();
        } else {
            current = next;
            next = moveToNextPair();
            return current;
        }
    }

    @Override
    protected void doClose() {
        for (final PairIteratorWithCurrent<K, V> iterator : iterators) {
            if (iterator.hasNext()) {
                iterator.close();
            }
        }
        iterators.clear();
        current = null;
        next = null;
    }

    @Override
    public Optional<Pair<K, V>> getCurrent() {
        return Optional.ofNullable(current);
    }

}
