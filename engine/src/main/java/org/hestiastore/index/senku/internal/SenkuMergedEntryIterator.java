package org.hestiastore.index.senku.internal;

import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.senku.SenkuMergeFunction;

/**
 * Lazy priority-queue merge over sorted entry iterators.
 */
final class SenkuMergedEntryIterator<K, V> extends AbstractCloseableResource
        implements EntryIterator<K, V> {

    private final Comparator<? super K> keyComparator;
    private final SenkuMergeFunction<K, V> mergeFunction;
    private final PriorityQueue<SenkuMergeCursor<K, V>> queue;

    SenkuMergedEntryIterator(
            final List<? extends EntryIterator<K, V>> iterators,
            final Comparator<? super K> keyComparator,
            final SenkuMergeFunction<K, V> mergeFunction) {
        final List<? extends EntryIterator<K, V>> validatedIterators = Vldtn
                .requireNonNull(iterators, "iterators");
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.mergeFunction = Vldtn.requireNonNull(mergeFunction,
                "mergeFunction");
        queue = new PriorityQueue<>(Math.max(1, validatedIterators.size()),
                this::compareCursors);
        initialize(validatedIterators);
    }

    @Override
    public boolean hasNext() {
        return !wasClosed() && !queue.isEmpty();
    }

    @Override
    public Entry<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        SenkuMergeCursor<K, V> activeCursor = null;
        try {
            activeCursor = queue.remove();
            final K key = activeCursor.current().getKey();
            V value = null;
            while (activeCursor != null) {
                value = drainKey(activeCursor, key, value);
                if (activeCursor.current() != null) {
                    queue.add(activeCursor);
                }
                activeCursor = pollEqualKey(key);
            }
            return Entry.of(key, Vldtn.requireNonNull(value, "mergedValue"));
        } catch (Exception e) {
            if (activeCursor != null) {
                try {
                    activeCursor.close();
                } catch (Exception cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                }
            }
            closeAfterFailure(e);
            if (e instanceof IndexException) {
                throw (IndexException) e;
            }
            throw new IndexException("Unable to merge sorted entries.", e);
        }
    }

    @Override
    protected void doClose() {
        IndexException failure = null;
        while (!queue.isEmpty()) {
            try {
                queue.remove().close();
            } catch (Exception e) {
                if (failure == null) {
                    failure = new IndexException(
                            "Unable to close sorted merge input.", e);
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private V drainKey(final SenkuMergeCursor<K, V> cursor, final K key,
            final V initialValue) {
        V value = initialValue;
        while (cursor.current() != null && keyComparator
                .compare(cursor.current().getKey(), key) == 0) {
            final V currentValue = cursor.current().getValue();
            value = value == null ? currentValue : Vldtn.requireNonNull(
                    mergeFunction.apply(key, value, currentValue),
                    "mergedValue");
            cursor.advance();
        }
        return value;
    }

    private SenkuMergeCursor<K, V> pollEqualKey(final K key) {
        final SenkuMergeCursor<K, V> cursor = queue.peek();
        if (cursor == null
                || keyComparator.compare(cursor.current().getKey(), key) != 0) {
            return null;
        }
        return queue.remove();
    }

    private int compareCursors(final SenkuMergeCursor<K, V> first,
            final SenkuMergeCursor<K, V> second) {
        final int compared = keyComparator.compare(first.current().getKey(),
                second.current().getKey());
        return compared == 0 ? Integer.compare(first.ordinal(), second.ordinal())
                : compared;
    }

    private void initialize(
            final List<? extends EntryIterator<K, V>> iterators) {
        int ordinal = 0;
        try {
            for (final EntryIterator<K, V> iterator : iterators) {
                final SenkuMergeCursor<K, V> cursor = new SenkuMergeCursor<>(
                        Vldtn.requireNonNull(iterator, "iterator"), ordinal++);
                if (cursor.current() != null) {
                    queue.add(cursor);
                }
            }
        } catch (Exception e) {
            closeInputs(iterators, e);
            throw e;
        }
    }

    private void closeAfterFailure(final Exception primary) {
        while (!queue.isEmpty()) {
            try {
                queue.remove().close();
            } catch (Exception cleanupFailure) {
                primary.addSuppressed(cleanupFailure);
            }
        }
    }

    private static <K, V> void closeInputs(
            final List<? extends EntryIterator<K, V>> iterators,
            final Exception primary) {
        for (final EntryIterator<K, V> iterator : iterators) {
            if (iterator == null || iterator.wasClosed()) {
                continue;
            }
            try {
                iterator.close();
            } catch (Exception cleanupFailure) {
                primary.addSuppressed(cleanupFailure);
            }
        }
    }
}
