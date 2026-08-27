package org.hestiastore.benchmark.senku;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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
 * Compares object-based Senku flush partitioning and sorting strategies without
 * filesystem or page-encoding cost.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
@State(Scope.Thread)
public class SenkuFlushPreparationBenchmark {

    private static final int INGESTION_STRIPE_COUNT = 32;
    private static final int STRIPE_MIX_MULTIPLIER_1 = 0x7feb352d;
    private static final int STRIPE_MIX_MULTIPLIER_2 = 0x846ca68b;
    private static final Comparator<Map.Entry<Long, Long>> ENTRY_COMPARATOR =
            (first, second) -> Long.compare(first.getKey(), second.getKey());

    @Param({ "1000000" })
    int entryCount = 1_000_000;

    @Param({ "32" })
    int shardCount = 32;

    private Long[] keys;
    private List<Map<Long, Long>> mutationStripes;
    private List<Map<Long, Long>> persistentShards;

    /**
     * Builds equivalent mutation-striped and persistent-shard-partitioned
     * inputs once per trial.
     */
    @Setup(Level.Trial)
    public void setup() {
        keys = new Long[entryCount];
        mutationStripes = newMaps(INGESTION_STRIPE_COUNT);
        persistentShards = newMaps(shardCount);
        for (int sequence = 0; sequence < entryCount; sequence++) {
            final long key = SenkuParallelIngestionBenchmark
                    .biasedBoardKey(sequence);
            final Long boxedKey = Long.valueOf(key);
            keys[sequence] = boxedKey;
            mutationStripes.get(stripe(key)).put(boxedKey, boxedKey);
            persistentShards.get(shardId(key)).put(boxedKey, boxedKey);
        }
        requireEntryCount(mutationStripes);
        requireEntryCount(persistentShards);
    }

    /**
     * Measures the production shape: count shards, fill one range-partitioned
     * reference array, and sort each shard range serially.
     *
     * @return checksum of sorted range boundaries
     */
    @Benchmark
    public long currentSingleArray() {
        final int[] counts = countShards();
        final int[] starts = starts(counts);
        final Map.Entry<Long, Long>[] ordered = orderSingleArray(starts);
        sortRanges(ordered, starts, counts);
        return checksum(ordered, starts, counts);
    }

    /**
     * Measures construction of the current 32 mutation-stripe maps from an
     * existing sequence of boxed keys.
     *
     * @return populated mutation-stripe maps
     */
    @Benchmark
    public List<Map<Long, Long>> buildMutationStripeMaps() {
        final List<Map<Long, Long>> maps = newMaps(INGESTION_STRIPE_COUNT);
        for (final Long key : keys) {
            maps.get(stripe(key.longValue())).put(key, key);
        }
        return maps;
    }

    /**
     * Measures construction of maps partitioned directly by persistent shard
     * from the same existing sequence of boxed keys.
     *
     * @return populated persistent-shard maps
     */
    @Benchmark
    public List<Map<Long, Long>> buildPersistentShardMaps() {
        final List<Map<Long, Long>> maps = newMaps(shardCount);
        for (final Long key : keys) {
            maps.get(shardId(key.longValue())).put(key, key);
        }
        return maps;
    }

    /**
     * Measures the proposed flush-time shape using one exactly sized reference
     * array per persistent shard.
     *
     * @return checksum of sorted shard boundaries
     */
    @Benchmark
    public long perShardArrays() {
        final int[] counts = countShards();
        final Map.Entry<Long, Long>[][] ordered = orderShardArrays(counts);
        for (int shardId = 0; shardId < shardCount; shardId++) {
            Arrays.sort(ordered[shardId], ENTRY_COMPARATOR);
        }
        return checksum(ordered);
    }

    /**
     * Measures flush preparation when ingestion has already retained one map
     * per persistent shard, excluding the changed ingestion cost.
     *
     * @return checksum of sorted shard boundaries
     */
    @Benchmark
    public long prepartitionedShardMaps() {
        long checksum = 0L;
        for (final Map<Long, Long> shard : persistentShards) {
            final Map.Entry<Long, Long>[] ordered = toArray(shard);
            Arrays.sort(ordered, ENTRY_COMPARATOR);
            checksum += checksum(ordered);
        }
        return checksum;
    }

    /**
     * Measures the production partitioning shape with independent shard ranges
     * sorted concurrently on the common fork-join pool.
     *
     * @return checksum of sorted range boundaries
     */
    @Benchmark
    public long concurrentShardSort() {
        final int[] counts = countShards();
        final int[] starts = starts(counts);
        final Map.Entry<Long, Long>[] ordered = orderSingleArray(starts);
        IntStream.range(0, shardCount).parallel().forEach(shardId -> {
            final int from = starts[shardId];
            Arrays.sort(ordered, from, from + counts[shardId],
                    ENTRY_COMPARATOR);
        });
        return checksum(ordered, starts, counts);
    }

    private List<Map<Long, Long>> newMaps(final int mapCount) {
        final int capacity = 1 + (initialCapacity() - 1) / mapCount;
        final List<Map<Long, Long>> maps = new ArrayList<>(mapCount);
        for (int index = 0; index < mapCount; index++) {
            maps.add(new HashMap<>(capacity));
        }
        return maps;
    }

    private int initialCapacity() {
        return (int) ((4L * entryCount + 2L) / 3L);
    }

    private int[] countShards() {
        final int[] counts = new int[shardCount];
        for (final Map<Long, Long> stripe : mutationStripes) {
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
    private Map.Entry<Long, Long>[] orderSingleArray(final int[] starts) {
        final Map.Entry<Long, Long>[] ordered = new Map.Entry[entryCount];
        final int[] next = Arrays.copyOf(starts, starts.length);
        for (final Map<Long, Long> stripe : mutationStripes) {
            for (final Map.Entry<Long, Long> entry : stripe.entrySet()) {
                final int shardId = shardId(entry.getKey().longValue());
                ordered[next[shardId]++] = entry;
            }
        }
        return ordered;
    }

    @SuppressWarnings("unchecked")
    private Map.Entry<Long, Long>[][] orderShardArrays(final int[] counts) {
        final Map.Entry<Long, Long>[][] ordered = new Map.Entry[shardCount][];
        for (int shardId = 0; shardId < shardCount; shardId++) {
            ordered[shardId] = new Map.Entry[counts[shardId]];
        }
        final int[] next = new int[shardCount];
        for (final Map<Long, Long> stripe : mutationStripes) {
            for (final Map.Entry<Long, Long> entry : stripe.entrySet()) {
                final int shardId = shardId(entry.getKey().longValue());
                ordered[shardId][next[shardId]++] = entry;
            }
        }
        return ordered;
    }

    private void sortRanges(final Map.Entry<Long, Long>[] ordered,
            final int[] starts, final int[] counts) {
        for (int shardId = 0; shardId < shardCount; shardId++) {
            final int from = starts[shardId];
            Arrays.sort(ordered, from, from + counts[shardId],
                    ENTRY_COMPARATOR);
        }
    }

    private long checksum(final Map.Entry<Long, Long>[] ordered,
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

    private long checksum(final Map.Entry<Long, Long>[][] ordered) {
        long checksum = 0L;
        for (final Map.Entry<Long, Long>[] shard : ordered) {
            checksum += checksum(shard);
        }
        return checksum;
    }

    private long checksum(final Map.Entry<Long, Long>[] ordered) {
        if (ordered.length == 0) {
            return 0L;
        }
        return ordered[0].getKey().longValue()
                + ordered[ordered.length - 1].getKey().longValue();
    }

    @SuppressWarnings("unchecked")
    private Map.Entry<Long, Long>[] toArray(final Map<Long, Long> entries) {
        return entries.entrySet().toArray(new Map.Entry[entries.size()]);
    }

    private int stripe(final long key) {
        int mixed = Long.hashCode(key);
        mixed ^= mixed >>> 16;
        mixed *= STRIPE_MIX_MULTIPLIER_1;
        mixed ^= mixed >>> 15;
        mixed *= STRIPE_MIX_MULTIPLIER_2;
        mixed ^= mixed >>> 16;
        return mixed & (INGESTION_STRIPE_COUNT - 1);
    }

    private int shardId(final long key) {
        return Math.floorMod(Long.hashCode(key), shardCount);
    }

    private void requireEntryCount(final List<Map<Long, Long>> maps) {
        int actual = 0;
        for (final Map<Long, Long> map : maps) {
            actual += map.size();
        }
        if (actual != entryCount) {
            throw new IllegalStateException(String.format(
                    "Expected '%d' entries but found '%d'.", entryCount,
                    actual));
        }
    }
}
