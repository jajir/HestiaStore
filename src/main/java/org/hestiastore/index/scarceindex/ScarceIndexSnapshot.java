package org.hestiastore.index.scarceindex;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.hestiastore.index.Pair;
import org.hestiastore.index.Vldtn;

/**
 * Immutable view of the scarce index cache content. Provides convenience
 * accessors that are useful for validation and testing without exposing the
 * underlying mutable cache state.
 */
final class ScarceIndexSnapshot<K> {

    private final Comparator<K> comparator;
    private final List<Pair<K, Integer>> entries;

    ScarceIndexSnapshot(final Comparator<K> comparator,
            final List<Pair<K, Integer>> entries) {
        this.comparator = Vldtn.requireNonNull(comparator, "comparator");
        Vldtn.requireNonNull(entries, "entries");
        this.entries = List.copyOf(entries);
    }

    int getKeyCount() {
        return entries.size();
    }

    K getMinKey() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.get(0).getKey();
    }

    K getMaxKey() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.get(entries.size() - 1).getKey();
    }

    Stream<Pair<K, Integer>> getSegments() {
        return entries.stream();
    }

    Integer findSegmentId(final K key) {
        Vldtn.requireNonNull(key, "key");
        if (entries.isEmpty()) {
            return null;
        }
        if (comparator.compare(key,
                entries.get(entries.size() - 1).getKey()) > 0) {
            return null;
        }
        for (final Pair<K, Integer> entry : entries) {
            if (comparator.compare(key, entry.getKey()) <= 0) {
                return entry.getValue();
            }
        }
        return null;
    }

    List<Pair<K, Integer>> entries() {
        return entries;
    }

}
