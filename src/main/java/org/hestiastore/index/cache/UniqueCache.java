package org.hestiastore.index.cache;

import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.Vldtn;
import org.hestiastore.index.sorteddatafile.PairComparator;

/**
 * Cache for index operation. When there are two operations with same key value
 * than just latest is stored. Because just last one is valid.
 * 
 * This cache doesn't support eviction. When is full that all data are evicted
 * at once.
 */
public class UniqueCache<K, V> {

    private final Comparator<K> keyComparator;
    private final TreeMap<K, V> map;

    public static <M, N> UniqueCacheBuilder<M, N> builder() {
        return new UniqueCacheBuilder<>();
    }

    public UniqueCache(final Comparator<K> keyComparator) {
        this.keyComparator = Vldtn.requireNonNull(keyComparator,
                "keyComparator");
        this.map = new TreeMap<>(keyComparator);
    }

    /**
     * When there is old value than old value is rewritten.
     */
    public void put(final Pair<K, V> pair) {
        map.merge(pair.getKey(), pair.getValue(), (oldVal, newVal) -> newVal);
    }

    public V get(final K key) {
        Vldtn.requireNonNull(key, "key");
        return map.get(key);
    }

    public void clear() {
        map.clear();
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * It's unsorted.
     * 
     * @return
     */
    public List<Pair<K, V>> toList() {
        return map.entrySet().stream()
                .map(entry -> new Pair<K, V>(entry.getKey(), entry.getValue()))
                .toList();
    }

    public List<Pair<K, V>> getAsSortedList() {
        return map.entrySet().stream()
                .map(entry -> new Pair<K, V>(entry.getKey(), entry.getValue()))
                .sorted(new PairComparator<>(keyComparator))//
                .toList();
    }

    public List<K> getSortedKeys() {
        return map.entrySet().stream()//
                .map(entry -> entry.getKey())//
                .sorted(keyComparator)//
                .toList();
    }

    public PairIterator<K, V> getSortedIterator() {
        return PairIterator.make(getAsSortedList().iterator());
    }

    /**
     * Get unsorted stream of key value pairs
     * 
     * @return unsorted stream of key value pairs
     */
    public Stream<Pair<K, V>> getStream() {
        return map.entrySet().stream()
                .map(entry -> new Pair<K, V>(entry.getKey(), entry.getValue()));
    }

}
