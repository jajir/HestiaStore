package org.hestiastore.index.senku.internal;

import static org.hestiastore.index.datatype.NullValue.NULL;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.hestiastore.benchmark.senku.SenkuParallelIngestionBenchmark;
import org.hestiastore.index.datatype.NullValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

/**
 * Measures the current striped {@link HashMap} ingestion layout independently
 * from flush, maintenance, filesystem, and final-drain costs.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = { "-Xms2g", "-Xmx2g" })
public class SenkuIngestionMapBenchmark {

    private static final int ENTRY_COUNT = 1_048_576;
    private static final int INGESTION_STRIPE_COUNT = 32;
    private static final String COLLISION_HEAVY = "collision-heavy";
    private static final String BIT_BOARD = "bit-board";
    private static final String RANDOMIZED = "randomized";

    /**
     * Builds one complete unique-key batch using the current mutation maps.
     * JMH normalizes throughput and allocation to one inserted entry.
     *
     * @param state precomputed keys outside the measured operation
     * @return populated mutation maps
     */
    @Benchmark
    @Threads(1)
    @OperationsPerInvocation(ENTRY_COUNT)
    public List<Map<Long, NullValue>> buildUniqueEntries(
            final UniqueBuildState state) {
        final List<Map<Long, NullValue>> maps = newMaps(state.entryCount);
        for (final Long key : state.keys) {
            maps.get(stripe(key.longValue())).put(key, NULL);
        }
        return maps;
    }

    /**
     * Measures eight callers updating existing keys through the same striped
     * maps and mutation locks. The fixed key set keeps memory bounded.
     *
     * @param state shared populated maps and locks
     * @param cursor calling thread's deterministic key cursor
     * @return existing non-null value
     */
    @Benchmark
    @Threads(8)
    public NullValue updateExistingEntry(final ExistingEntryState state,
            final KeyCursor cursor) {
        return state.updateExisting(
                cursor.nextIndex(state.entryMask()));
    }

    /**
     * Precomputes one bounded unique-key batch per benchmark thread.
     */
    @State(Scope.Thread)
    public static class UniqueBuildState {

        @Param({ COLLISION_HEAVY, BIT_BOARD, RANDOMIZED })
        String distribution = COLLISION_HEAVY;

        int entryCount = ENTRY_COUNT;
        private Long[] keys;

        /**
         * Generates boxed keys outside the measured map construction.
         */
        @Setup(Level.Trial)
        public void setup() {
            keys = SenkuIngestionMapBenchmark.keys(distribution, entryCount);
        }
    }

    /**
     * Owns shared populated maps and locks for bounded duplicate updates.
     */
    @State(Scope.Benchmark)
    public static class ExistingEntryState {

        @Param({ COLLISION_HEAVY, BIT_BOARD, RANDOMIZED })
        String distribution = COLLISION_HEAVY;

        int entryCount = ENTRY_COUNT;
        private Long[] keys;
        private List<Map<Long, NullValue>> maps;
        private ReentrantLock[] locks;

        /**
         * Populates the fixed key set and creates one lock per mutation stripe.
         */
        @Setup(Level.Trial)
        public void setup() {
            requirePowerOfTwo(entryCount);
            keys = keys(distribution, entryCount);
            maps = newMaps(entryCount);
            locks = new ReentrantLock[INGESTION_STRIPE_COUNT];
            for (int index = 0; index < locks.length; index++) {
                locks[index] = new ReentrantLock();
            }
            for (final Long key : keys) {
                maps.get(stripe(key.longValue())).put(key, NULL);
            }
        }

        int entryMask() {
            return entryCount - 1;
        }

        int size() {
            return maps.stream().mapToInt(Map::size).sum();
        }

        private NullValue updateExisting(final int keyIndex) {
            final Long key = keys[keyIndex];
            final int stripe = stripe(key.longValue());
            final ReentrantLock lock = locks[stripe];
            lock.lock();
            try {
                final Map<Long, NullValue> map = maps.get(stripe);
                final NullValue current = map.get(key);
                if (current == null) {
                    throw new IllegalStateException(
                            "Expected an existing benchmark key.");
                }
                map.put(key, current);
                return current;
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Generates a disjoint deterministic access sequence for each JMH thread.
     */
    @State(Scope.Thread)
    public static class KeyCursor {

        private long sequence;
        private long stride = 1L;

        /**
         * Assigns a different start and odd stride to each benchmark thread.
         *
         * @param threadParams JMH thread identity
         */
        @Setup(Level.Trial)
        public void setup(final ThreadParams threadParams) {
            reset(threadParams.getThreadIndex());
        }

        /**
         * Resets the deterministic sequence for a selected caller identity.
         *
         * @param threadIndex zero-based caller identity
         */
        void reset(final int threadIndex) {
            sequence = threadIndex;
            stride = 2L * threadIndex + 1L;
        }

        /**
         * Returns the next bounded index and advances this caller's sequence.
         *
         * @param mask entry-count mask
         * @return next key-array index
         */
        int nextIndex(final int mask) {
            final int index = (int) sequence & mask;
            sequence += stride;
            return index;
        }
    }

    private static List<Map<Long, NullValue>> newMaps(final int entryCount) {
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

    private static Long[] keys(final String distribution,
            final int entryCount) {
        final Long[] keys = new Long[entryCount];
        for (int index = 0; index < entryCount; index++) {
            keys[index] = Long.valueOf(key(distribution, index));
        }
        return keys;
    }

    private static long key(final String distribution, final long sequence) {
        if (COLLISION_HEAVY.equals(distribution)) {
            return collisionHeavyKey(sequence);
        }
        if (BIT_BOARD.equals(distribution)) {
            return SenkuParallelIngestionBenchmark.biasedBoardKey(sequence);
        }
        if (RANDOMIZED.equals(distribution)) {
            return randomizedKey(sequence);
        }
        throw new IllegalArgumentException(
                "Unknown key distribution: " + distribution);
    }

    private static long collisionHeavyKey(final long sequence) {
        final int lowBucketBits = (int) sequence & 0x3ff;
        final int upperHashBits = (int) (sequence >>> 10) & 0xffff;
        final int hash = upperHashBits << 16
                | (upperHashBits ^ lowBucketBits);
        final int cycle = (int) (sequence >>> 26);
        final int lowWord = hash ^ cycle;
        return (long) cycle << 32 | Integer.toUnsignedLong(lowWord);
    }

    private static long randomizedKey(final long sequence) {
        long value = sequence + 0x9e37_79b9_7f4a_7c15L;
        value = (value ^ value >>> 30) * 0xbf58_476d_1ce4_e5b9L;
        value = (value ^ value >>> 27) * 0x94d0_49bb_1331_11ebL;
        return value ^ value >>> 31;
    }

    private static int stripe(final long key) {
        return SenkuIngestor.stripeFromHash(Long.hashCode(key));
    }

    private static void requirePowerOfTwo(final int value) {
        if (value <= 0 || (value & (value - 1)) != 0) {
            throw new IllegalArgumentException(
                    "Benchmark entry count must be a power of two.");
        }
    }
}
