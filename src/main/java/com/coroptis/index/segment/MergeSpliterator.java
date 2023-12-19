package com.coroptis.index.segment;

import java.util.Comparator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.datatype.TypeDescriptor;

/**
 * Allows to create final stream of data from cache and SST. Tombstones are
 * applied.
 * 
 * @author honza
 *
 * @param <K>
 * @param <V>
 */
public class MergeSpliterator<K, V> implements Spliterator<Pair<K, V>> {

    private final PairIterator<K, V> sstFile;

    private final PairIterator<K, V> cache;

    final TypeDescriptor<V> valueTypeDescriptor;

    private final Comparator<K> keyComparator;

    MergeSpliterator(final PairIterator<K, V> sstFile,
            final PairIterator<K, V> cache,
            final TypeDescriptor<K> keyTypeDescriptor,
            final TypeDescriptor<V> valueTypeDescriptor) {
        this.sstFile = Objects.requireNonNull(sstFile);
        this.cache = Objects.requireNonNull(cache);
        this.valueTypeDescriptor = Objects.requireNonNull(valueTypeDescriptor);
        Objects.requireNonNull(keyTypeDescriptor);
        this.keyComparator = keyTypeDescriptor.getComparator();
    }

    @Override
    public int characteristics() {
        return Spliterator.SORTED | Spliterator.NONNULL | Spliterator.DISTINCT
                | Spliterator.IMMUTABLE;
    }

    @Override
    public long estimateSize() {
        /*
         * Size is not known.
         */
        return Long.MAX_VALUE;
    }

    @Override
    public boolean tryAdvance(final Consumer<? super Pair<K, V>> consumer) {
        if (sstFile.hasNext()) {
            if (cache.hasNext()) {
                final Pair<K, V> pSst = sstFile.readCurrent().get();
                final Pair<K, V> pCache = cache.readCurrent().get();
                final int cmp = keyComparator.compare(pSst.getKey(),
                        pCache.getKey());
                if (cmp < 0) {
                    consumer.accept(pSst);
                    sstFile.next();
                } else {
                    if (cmp == 0) {
                        if (!valueTypeDescriptor
                                .isTombstone(pCache.getValue())) {
                            consumer.accept(pCache);
                        }
                        sstFile.next();
                        cache.next();
                    } else {
                        // cmp >0
                        consumer.accept(pCache);
                        cache.next();
                    }
                }
            } else {
                final Pair<K, V> out = sstFile.next();
                /*
                 * Check for tombstones is not required, because tombstones are
                 * not stored in index.
                 */
                consumer.accept(out);
            }
        } else {
            if (cache.hasNext()) {
                final Pair<K, V> out = cache.next();
                if (!valueTypeDescriptor.isTombstone(out.getValue())) {
                    consumer.accept(out);
                }
            } else {
                // no more records
                return false;
            }
        }
        return true;
    }

    @Override
    public Comparator<? super Pair<K, V>> getComparator() {
        return (pair1, pair2) -> keyComparator.compare(pair1.getKey(),
                pair2.getKey());
    }

    @Override
    public Spliterator<Pair<K, V>> trySplit() {
        return null;
    }
}
