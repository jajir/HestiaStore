package com.coroptis.index.sstfile;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIteratorWithCurrent;

public class MergedPairIterator<K, V> implements PairIteratorWithCurrent<K, V> {

        private final List<PairIteratorWithCurrent<K, V>> iterators;
        private final Comparator<K> keyComparator;
        private final Merger<K, V> merger;
        private Pair<K, V> current;
        private Pair<K, V> next;

        MergedPairIterator(final List<PairIteratorWithCurrent<K, V>> iterators,
                        final Comparator<K> keyComparator,
                        final Merger<K, V> merger) {
                Objects.requireNonNull(iterators, "iterators must not be null");
                this.iterators = new ArrayList<>();
                this.iterators.addAll(iterators);
                this.keyComparator = Objects.requireNonNull(keyComparator,
                                "keyComparator must not be null");
                this.merger = Objects.requireNonNull(merger,
                                "merger must not be null");
                next = moveToNextPair();
        }

        private Pair<K, V> moveToNextPair() {
                final Optional<PairIteratorWithCurrent<K, V>> oLowestIter = findIteratorWithLowestKey();
                if (!oLowestIter.isPresent()) {
                        return null;
                }
                final PairIteratorWithCurrent<K, V> lowestIter = oLowestIter
                                .get();
                if (lowestIter.getCurrent().isPresent()) {
                        final K lowestKey = oLowestIter.get().getCurrent().get()
                                        .getKey();
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
                                        if (comparator.compare(iterator,
                                                        lowest) < 0) {
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
                Objects.requireNonNull(key);
                V out = null;
                for (final PairIteratorWithCurrent<K, V> iter : iterators) {
                        if (iter.getCurrent().isPresent()) {
                                final Pair<K, V> pair = iter.getCurrent().get();
                                final K k = pair.getKey();
                                if (keyComparator.compare(k, key) == 0) {
                                        if (out == null) {
                                                out = pair.getValue();
                                        } else {
                                                out = merger.merge(key, out,
                                                                pair.getValue());
                                        }
                                        if (iter.hasNext()) {
                                                iter.next();
                                        } else {
                                                iter.close();
                                        }
                                }
                        }
                }
                Objects.requireNonNull(out, "out must not be null");
                return new Pair<K, V>(key, out);
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
        public void close() {
        }

        @Override
        public Optional<Pair<K, V>> getCurrent() {
                return Optional.ofNullable(current);
        }

}
