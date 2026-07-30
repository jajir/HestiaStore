package org.hestiastore.index.segment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Protects the unbounded segment merge loop from range-scan overhead.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class SegmentMergeSequentialBenchmark {

    private static final TypeDescriptor<Integer> KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptor<String> VALUE_DESCRIPTOR = new TypeDescriptorShortString();

    @Param({ "32768" })
    private int keyCount;

    private List<Entry<Integer, String>> entries;
    private long expectedKeySum;

    /**
     * Creates the immutable entry set consumed by each benchmark invocation.
     */
    @Setup(Level.Trial)
    public void setup() {
        final List<Entry<Integer, String>> preparedEntries = new ArrayList<>(
                keyCount);
        for (int key = 0; key < keyCount; key++) {
            preparedEntries.add(Entry.of(key, "value"));
        }
        entries = List.copyOf(preparedEntries);
        expectedKeySum = (long) keyCount * (keyCount - 1L) / 2L;
    }

    /**
     * Measures the existing unbounded merge path with an empty delta cache.
     *
     * @return verified sum of all keys
     */
    @Benchmark
    public long mergeSequential() {
        try (EntryIterator<Integer, String> iterator = new MergeDeltaCacheWithIndexIterator<>(
                new EntryIteratorList<>(entries.iterator()), KEY_DESCRIPTOR,
                VALUE_DESCRIPTOR, Collections.emptyIterator())) {
            long keySum = 0L;
            int count = 0;
            while (iterator.hasNext()) {
                keySum += iterator.next().getKey();
                count++;
            }
            if (count != keyCount || keySum != expectedKeySum) {
                throw new IllegalStateException(
                        "Sequential merge returned incomplete data.");
            }
            return keySum;
        }
    }
}
