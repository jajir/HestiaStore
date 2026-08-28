package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.datatype.NullValue.NULL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;

import org.hestiastore.benchmark.senku.SenkuParallelIngestionBenchmark;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Compares the former entry-array TimSort flush preparation with compact
 * primitive-long ordering on the collision-heavy peg-solitaire key shape.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
@State(Scope.Thread)
public class SenkuFlushOrderingBenchmark {

    private static final int INGESTION_STRIPE_COUNT = 32;
    private static final int SORT_PARALLELISM = 4;
    private static final Comparator<Map.Entry<Long, NullValue>> COMPARATOR =
            (first, second) -> Long.compare(first.getKey().longValue(),
                    second.getKey().longValue());
    private static final TypeDescriptorLong KEYS = new TypeDescriptorLong();
    private static final TypeDescriptorNull VALUES = new TypeDescriptorNull();

    @Param({ "1000000" })
    int entryCount = 1_000_000;

    @Param({ "32" })
    int shardCount = 32;

    private List<Map<Long, NullValue>> mutationStripes;
    private ForkJoinPool baselinePool;

    /**
     * Builds collision-heavy detached mutation stripes outside measurement.
     */
    @Setup(Level.Trial)
    public void setup() {
        baselinePool = new ForkJoinPool(SORT_PARALLELISM);
        mutationStripes = newMaps();
        for (int sequence = 0; sequence < entryCount; sequence++) {
            final long key = SenkuParallelIngestionBenchmark
                    .biasedBoardKey(sequence);
            mutationStripes.get(stripe(key)).put(Long.valueOf(key), NULL);
        }
    }

    /**
     * Stops the benchmark-only baseline executor.
     */
    @TearDown(Level.Trial)
    public void tearDown() {
        baselinePool.shutdown();
    }

    /**
     * Measures the former full {@code Map.Entry[]} representation with four
     * explicitly bounded TimSort tasks.
     *
     * @return sorted-boundary checksum
     */
    @Benchmark
    public long entryArrayBoundedTimSort() {
        final int[] counts = countShards();
        final int[] starts = starts(counts);
        final Map.Entry<Long, NullValue>[] ordered = orderEntries(starts);
        final List<ForkJoinTask<?>> tasks = new ArrayList<>(shardCount);
        for (int shardId = 0; shardId < shardCount; shardId++) {
            final int from = starts[shardId];
            final int to = from + counts[shardId];
            tasks.add(baselinePool
                    .submit(() -> Arrays.sort(ordered, from, to, COMPARATOR)));
        }
        for (final ForkJoinTask<?> task : tasks) {
            task.join();
        }
        return checksum(ordered, starts, counts);
    }

    /**
     * Measures compact primitive-long partitioning and in-place serial sorting.
     *
     * @return sorted-boundary checksum
     */
    @Benchmark
    public long compactSerialSort() {
        return compactSort(false);
    }

    /**
     * Measures the production compact representation and globally bounded
     * parallel sorter.
     *
     * @return sorted-boundary checksum
     */
    @Benchmark
    public long compactBoundedSort() {
        return compactSort(true);
    }

    private long compactSort(final boolean parallel) {
        final int[] counts = countShards();
        final int[] starts = starts(counts);
        final SenkuFlushOrder<Long, NullValue> ordered = new SenkuFlushOrder<>(
                KEYS, VALUES, entryCount);
        final int[] next = starts.clone();
        for (final Map<Long, NullValue> stripe : mutationStripes) {
            stripe.forEach((key, value) -> {
                final int shardId = shardId(key.longValue());
                ordered.set(next[shardId]++, key, value);
            });
        }
        SenkuFlushSortTask.sortAll(ordered, starts, counts, parallel);
        return checksum(ordered, starts, counts);
    }

    private List<Map<Long, NullValue>> newMaps() {
        final int totalCapacity = (int) ((4L * entryCount + 2L) / 3L);
        final int capacity = 1
                + (totalCapacity - 1) / INGESTION_STRIPE_COUNT;
        final List<Map<Long, NullValue>> maps = new ArrayList<>(
                INGESTION_STRIPE_COUNT);
        for (int index = 0; index < INGESTION_STRIPE_COUNT; index++) {
            maps.add(new HashMap<>(capacity));
        }
        return maps;
    }

    private int[] countShards() {
        final int[] counts = new int[shardCount];
        for (final Map<Long, NullValue> stripe : mutationStripes) {
            for (final Long key : stripe.keySet()) {
                counts[shardId(key.longValue())]++;
            }
        }
        return counts;
    }

    private int[] starts(final int[] counts) {
        final int[] starts = new int[shardCount];
        int next = 0;
        for (int shardId = 0; shardId < shardCount; shardId++) {
            starts[shardId] = next;
            next += counts[shardId];
        }
        return starts;
    }

    @SuppressWarnings("unchecked")
    private Map.Entry<Long, NullValue>[] orderEntries(final int[] starts) {
        final Map.Entry<Long, NullValue>[] ordered =
                new Map.Entry[entryCount];
        final int[] next = starts.clone();
        for (final Map<Long, NullValue> stripe : mutationStripes) {
            for (final Map.Entry<Long, NullValue> entry : stripe.entrySet()) {
                final int shardId = shardId(entry.getKey().longValue());
                ordered[next[shardId]++] = entry;
            }
        }
        return ordered;
    }

    private long checksum(final Map.Entry<Long, NullValue>[] ordered,
            final int[] starts, final int[] counts) {
        long checksum = 0L;
        for (int shardId = 0; shardId < shardCount; shardId++) {
            final int count = counts[shardId];
            if (count > 0) {
                final int from = starts[shardId];
                checksum += ordered[from].getKey().longValue();
                checksum += ordered[from + count - 1].getKey().longValue();
            }
        }
        return checksum;
    }

    private long checksum(final SenkuFlushOrder<Long, NullValue> ordered,
            final int[] starts, final int[] counts) {
        long checksum = 0L;
        for (int shardId = 0; shardId < shardCount; shardId++) {
            final int count = counts[shardId];
            if (count > 0) {
                final int from = starts[shardId];
                checksum += ordered.longKey(from);
                checksum += ordered.longKey(from + count - 1);
            }
        }
        return checksum;
    }

    private int stripe(final long key) {
        int mixed = Long.hashCode(key);
        mixed ^= mixed >>> 16;
        mixed *= 0x7feb352d;
        mixed ^= mixed >>> 15;
        mixed *= 0x846ca68b;
        mixed ^= mixed >>> 16;
        return mixed & (INGESTION_STRIPE_COUNT - 1);
    }

    private int shardId(final long key) {
        return Math.floorMod(Long.hashCode(key), shardCount);
    }
}
