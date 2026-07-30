package org.hestiastore.benchmark.cache;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Entry;
import org.hestiastore.index.cache.UniqueCache;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares the sorted key snapshot with the previous
 * {@code ArrayList(Collection)} implementation.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class UniqueCacheSortedKeyIteratorBenchmark {

    private static final Comparator<Integer> KEY_COMPARATOR = Integer::compare;
    private static final int PERMUTATION_MULTIPLIER = 0x9E3779B9;

    @Param({ "100000", "500000" })
    private int numberOfKeys;

    private UniqueCache<Integer, Integer> cache;
    private Map<Integer, Integer> legacyMap;

    /**
     * Builds equivalent caches and validates output parity before measurement.
     */
    @Setup(Level.Trial)
    public void setup() {
        cache = UniqueCache.<Integer, Integer>builder()
                .withKeyComparator(KEY_COMPARATOR)
                .withInitialCapacity(numberOfKeys)
                .buildEmpty();
        legacyMap = new ConcurrentHashMap<>(numberOfKeys, 0.75f, 1);
        for (int index = 0; index < numberOfKeys; index++) {
            final Integer key = permutedKey(index);
            cache.put(Entry.of(key, key));
            legacyMap.put(key, key);
        }
        validateParity();
    }

    /**
     * Measures the previous key-set copy into a second array before sorting.
     *
     * @return iterator over the legacy sorted snapshot
     */
    @Benchmark
    public Iterator<Integer> legacyArrayListSnapshot() {
        final List<Integer> keys = new ArrayList<>(legacyMap.keySet());
        keys.sort(KEY_COMPARATOR);
        return keys.iterator();
    }

    /**
     * Measures the production implementation that sorts the key-set array.
     *
     * @return iterator over the current sorted snapshot
     */
    @Benchmark
    public Iterator<Integer> reusedArraySnapshot() {
        return cache.getSortedKeyIterator();
    }

    /**
     * Verifies both implementations return the same ordered keys.
     */
    private void validateParity() {
        final Iterator<Integer> legacy = legacyArrayListSnapshot();
        final Iterator<Integer> current = reusedArraySnapshot();
        int count = 0;
        while (legacy.hasNext() && current.hasNext()) {
            final Integer legacyKey = legacy.next();
            final Integer currentKey = current.next();
            if (!legacyKey.equals(currentKey)) {
                throw new IllegalStateException(String.format(
                        "Key mismatch at index '%d': legacy '%d', current '%d'",
                        count, legacyKey, currentKey));
            }
            count++;
        }
        if (legacy.hasNext() || current.hasNext() || count != numberOfKeys) {
            throw new IllegalStateException(String.format(
                    "Iterator size mismatch: expected '%d', compared '%d'",
                    numberOfKeys, count));
        }
    }

    /**
     * Produces each key exactly once in a deterministic non-sequential order.
     *
     * @param index source index
     * @return permuted key
     */
    private int permutedKey(final int index) {
        return Integer.rotateLeft(index * PERMUTATION_MULTIPLIER, 16);
    }
}
