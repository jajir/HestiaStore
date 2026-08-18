package org.hestiastore.index.segment;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
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
 * Measures the cost of clearing the in-memory caches during segment close.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class SegmentCacheEvictAllBenchmark {

    private static final Comparator<Integer> KEY_COMPARATOR = Integer::compare;
    private static final TypeDescriptorShortString VALUE_DESCRIPTOR =
            new TypeDescriptorShortString();

    @Param({ "0", "1000", "10000", "100000" })
    private int numberOfKeys;

    private List<Entry<Integer, String>> deltaEntries;
    private SegmentCache<Integer, String> cache;

    /**
     * Builds reusable entries outside invocation-level setup.
     */
    @Setup(Level.Trial)
    public void setupEntries() {
        final int deltaSize = numberOfKeys / 2;
        deltaEntries = new ArrayList<>(deltaSize);
        for (int key = 0; key < deltaSize; key++) {
            deltaEntries.add(Entry.of(key, Integer.toString(key)));
        }
    }

    /**
     * Rebuilds a representative cache and freezes writes as close does.
     */
    @Setup(Level.Invocation)
    public void setupCache() {
        final int capacity = Math.max(1, numberOfKeys);
        cache = new SegmentCache<>(KEY_COMPARATOR, VALUE_DESCRIPTOR,
                deltaEntries, capacity, capacity, capacity);
        for (int key = numberOfKeys / 2; key < numberOfKeys; key++) {
            cache.putToWriteCache(Entry.of(key, Integer.toString(key)));
        }
        cache.freezeWriteCache();
    }

    /**
     * Measures clearing all segment cache layers.
     */
    @Benchmark
    public void evictAll() {
        cache.evictAll();
    }
}
