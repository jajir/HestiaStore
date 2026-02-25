package org.hestiastore.index.scarceindex;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.Vldtn;

/**
 * Immutable view of the scarce index cache content. Provides convenience
 * accessors that are useful for validation and testing without exposing the
 * underlying mutable cache state.
 */
final class ScarceIndexSnapshot<K> {

    private final Comparator<K> comparator;
    private final Entry<K, Integer>[] entries;

    ScarceIndexSnapshot(final Comparator<K> comparator,
            final List<Entry<K, Integer>> entries) {
        this.comparator = Vldtn.requireNonNull(comparator, "comparator");
        final List<Entry<K, Integer>> copiedEntries = List
                .copyOf(Vldtn.requireNonNull(entries, "entries"));
        @SuppressWarnings("unchecked")
        final Entry<K, Integer>[] array = copiedEntries.toArray(new Entry[0]);
        this.entries = array;
    }

    int getKeyCount() {
        return entries.length;
    }

    K getMinKey() {
        if (entries.length == 0) {
            return null;
        }
        return entries[0].getKey();
    }

    K getMaxKey() {
        if (entries.length == 0) {
            return null;
        }
        return entries[entries.length - 1].getKey();
    }

    Stream<Entry<K, Integer>> getSegments() {
        return Arrays.stream(entries);
    }

    Integer findSegmentId(final K key) {
        Vldtn.requireNonNull(key, "key");
        if (entries.length == 0) {
            return null;
        }
        if (comparator.compare(key, entries[entries.length - 1].getKey()) > 0) {
            return null;
        }
        int left = 0;
        int right = entries.length - 1;
        while (left < right) {
            final int middle = left + ((right - left) >>> 1);
            if (comparator.compare(key, entries[middle].getKey()) <= 0) {
                right = middle;
            } else {
                left = middle + 1;
            }
        }
        return entries[left].getValue();
    }

    void forEachAdjacentPair(
            final BiConsumer<Entry<K, Integer>, Entry<K, Integer>> consumer) {
        final BiConsumer<Entry<K, Integer>, Entry<K, Integer>> validatedConsumer = Vldtn
                .requireNonNull(consumer, "consumer");
        for (int i = 1; i < entries.length; i++) {
            validatedConsumer.accept(entries[i - 1], entries[i]);
        }
    }

}
