package org.hestiastore.index.segmentindex.partition;

import java.util.NavigableMap;
import java.util.TreeMap;

import org.hestiastore.index.Vldtn;

/**
 * Immutable snapshot of a partition write batch waiting for drain.
 *
 * @param <K> key type
 * @param <V> value type
 * @author honza
 */
public final class PartitionImmutableRun<K, V> {

    private final NavigableMap<K, V> entries;

    PartitionImmutableRun(final NavigableMap<K, V> entries) {
        Vldtn.requireNonNull(entries, "entries");
        this.entries = java.util.Collections.unmodifiableNavigableMap(
                new TreeMap<>(entries));
    }

    public NavigableMap<K, V> getEntries() {
        return entries;
    }

    public int size() {
        return entries.size();
    }
}
