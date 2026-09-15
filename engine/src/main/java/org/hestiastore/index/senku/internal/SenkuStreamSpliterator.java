package org.hestiastore.index.senku.internal;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.sorteddatafile.EntryComparator;

/**
 * Unsplittable sorted spliterator over one Senku entry iterator.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SenkuStreamSpliterator<K, V>
        implements Spliterator<Entry<K, V>> {

    private final EntryIterator<K, V> iterator;
    private final Comparator<? super Entry<K, V>> comparator;

    SenkuStreamSpliterator(final EntryIterator<K, V> iterator,
            final Comparator<? super K> keyComparator) {
        this.iterator = Vldtn.requireNonNull(iterator, "iterator");
        comparator = new EntryComparator<>(
                Vldtn.requireNonNull(keyComparator, "keyComparator"));
    }

    @Override
    public boolean tryAdvance(final Consumer<? super Entry<K, V>> action) {
        final Consumer<? super Entry<K, V>> validatedAction = Vldtn
                .requireNonNull(action, "action");
        if (!iterator.hasNext()) {
            return false;
        }
        validatedAction.accept(iterator.next());
        return true;
    }

    @Override
    public Spliterator<Entry<K, V>> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return DISTINCT | IMMUTABLE | NONNULL | ORDERED | SORTED;
    }

    @Override
    public Comparator<? super Entry<K, V>> getComparator() {
        return comparator;
    }
}
