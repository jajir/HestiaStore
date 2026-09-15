package org.hestiastore.index.senku.internal;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.Vldtn;

/**
 * Reusable priority-queue head for one sorted merge input.
 */
final class SenkuMergeCursor<K, V> {

    private final EntryIterator<K, V> iterator;
    private final int ordinal;
    private Entry<K, V> current;

    SenkuMergeCursor(final EntryIterator<K, V> iterator, final int ordinal) {
        this.iterator = Vldtn.requireNonNull(iterator, "iterator");
        this.ordinal = Vldtn.requireGreaterThanOrEqualToZero(ordinal,
                "ordinal");
        advance();
    }

    Entry<K, V> current() {
        return current;
    }

    int ordinal() {
        return ordinal;
    }

    void advance() {
        if (iterator.hasNext()) {
            current = Vldtn.requireNonNull(iterator.next(), "entry");
            Vldtn.requireNonNull(current.getKey(), "key");
            Vldtn.requireNonNull(current.getValue(), "value");
            return;
        }
        current = null;
        close();
    }

    void close() {
        if (!iterator.wasClosed()) {
            iterator.close();
        }
    }
}
