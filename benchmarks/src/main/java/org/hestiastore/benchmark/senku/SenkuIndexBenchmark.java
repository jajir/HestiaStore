package org.hestiastore.benchmark.senku;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.senku.SenkuIndex;
import org.hestiastore.index.senku.SenkuMergeFunctionRegistry;
import org.hestiastore.index.senku.SenkuReady;
import org.hestiastore.index.senku.SenkuWriting;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

/**
 * Measures the six first-version Senku performance boundaries through its
 * public API and an in-memory directory.
 */
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class SenkuIndexBenchmark {

    /**
     * Measures lock-protected ingestion before finalization.
     *
     * @param state       benchmark index
     * @param threadState disjoint per-thread key sequence
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(iterations = 3, time = 40, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = 7, time = 40,
            timeUnit = TimeUnit.MILLISECONDS)
    @Threads(4)
    public void ingest(final IngestState state,
            final IngestThreadState threadState) {
        state.put(threadState.nextKey());
    }

    /**
     * Measures the caller-visible put that reaches the map limit and flushes.
     *
     * @param state prefilled writing index
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Threads(1)
    public void synchronousFlush(final SynchronousFlushState state) {
        state.flush();
    }

    /**
     * Measures finalization through either flush-to-L0 or one recursive run
     * merge, selected by the state parameter.
     *
     * @param state prepared flush files
     * @return finalized handle, consumed by JMH
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Threads(1)
    public SenkuReady<Integer, Long> finishMaintenance(
            final MaintenanceState state) {
        return state.finish();
    }

    /**
     * Measures one complete globally sorted ready stream.
     *
     * @param state finalized index
     * @return number of streamed entries
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Threads(1)
    public long readyStream(final ReadyState state) {
        return state.stream();
    }

    /**
     * Measures ingestion through receipt of the first globally sorted entry.
     *
     * @param state workload shape
     * @return first sorted entry
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Threads(1)
    public Entry<Integer, Long> ingestToFirstSortedResult(
            final EndToEndState state) {
        return state.run();
    }

    private static SenkuWriting<Integer, Long> createWriting(
            final MemDirectory directory, final int maxInMemoryEntries,
            final int maxKeysPerPage, final long maxEntriesPerPart,
            final int mergeFanIn, final String shardDistribution) {
        final SenkuMergeFunctionRegistry<Integer, Long> functions =
                new SenkuMergeFunctionRegistry<>();
        functions.register((key, first, second) -> first + second);
        return SenkuIndex
                .builder(directory, new TypeDescriptorInteger(),
                        new TypeDescriptorLong(), functions)
                .shardHashFunction(key -> "skewed".equals(shardDistribution)
                        ? 0
                        : key.intValue())
                .shardCount(8).maxInMemoryEntries(maxInMemoryEntries)
                .maxKeysPerPage(maxKeysPerPage).mergeFanIn(mergeFanIn)
                .maintenanceThreads(2).maintenanceQueueSize(4)
                .diskIoBufferSize(1_024)
                .maxEntriesPerPart(maxEntriesPerPart).create();
    }

    private static void close(final SenkuReady<Integer, Long> ready) {
        if (ready != null) {
            ready.close();
        }
    }

    /**
     * Long-lived state for ingestion throughput.
     */
    @State(Scope.Benchmark)
    public static class IngestState {

        private SenkuWriting<Integer, Long> writing;

        /**
         * Opens a fresh writing index for one iteration.
         */
        @Setup(Level.Iteration)
        public void setup() {
            writing = createWriting(new MemDirectory(), 1_000_000, 4_096,
                    1_000_000L, 64, "balanced");
        }

        /**
         * Adds one unique entry.
         *
         * @param key unique key assigned to the calling benchmark thread
         */
        public void put(final int key) {
            writing.put(Integer.valueOf(key), Long.valueOf(key));
        }

        /**
         * Finalizes the index so no non-daemon thread survives the iteration.
         */
        @TearDown(Level.Iteration)
        public void tearDown() {
            close(writing.finishWriting());
            writing = null;
        }
    }

    /**
     * Per-thread disjoint key sequence that avoids benchmarking a shared key
     * counter instead of concurrent Senku ingestion.
     */
    @State(Scope.Thread)
    public static class IngestThreadState {

        private int nextKey;
        private int stride = 1;

        /**
         * Assigns one interleaved key sequence to each benchmark thread.
         *
         * @param threadParams JMH thread identity and group size
         */
        @Setup(Level.Iteration)
        public void setup(final ThreadParams threadParams) {
            nextKey = threadParams.getThreadIndex();
            stride = threadParams.getThreadCount();
        }

        /**
         * Returns the next key assigned to this benchmark thread.
         *
         * @return next unique key
         */
        int nextKey() {
            final int key = nextKey;
            nextKey += stride;
            return key;
        }
    }

    /**
     * Invocation state whose measured put performs a synchronous flush.
     */
    @State(Scope.Thread)
    public static class SynchronousFlushState {

        private static final int FLUSH_ENTRIES = 64;

        private SenkuWriting<Integer, Long> writing;

        /**
         * Prefills the map to one entry below its configured limit.
         */
        @Setup(Level.Invocation)
        public void setup() {
            writing = createWriting(new MemDirectory(), FLUSH_ENTRIES, 16, 32L,
                    8, "balanced");
            for (int key = 0; key < FLUSH_ENTRIES - 1; key++) {
                writing.put(Integer.valueOf(key), Long.valueOf(key));
            }
        }

        /**
         * Adds the entry that reaches the flush threshold.
         */
        public void flush() {
            writing.put(Integer.valueOf(FLUSH_ENTRIES),
                    Long.valueOf(FLUSH_ENTRIES));
        }

        /**
         * Finalizes the remaining empty map and releases all resources.
         */
        @TearDown(Level.Invocation)
        public void tearDown() {
            close(writing.finishWriting());
            writing = null;
        }
    }

    /**
     * Invocation state for L0 creation and recursive run merging.
     */
    @State(Scope.Thread)
    public static class MaintenanceState {

        @Param({ "flush-to-l0", "run-merge" })
        String scenario = "flush-to-l0";

        private SenkuWriting<Integer, Long> writing;
        private SenkuReady<Integer, Long> ready;

        /**
         * Produces finished one-entry flush files without waiting for the
         * periodic coordinator scan.
         */
        @Setup(Level.Invocation)
        public void setup() {
            final boolean runMerge = "run-merge".equals(scenario);
            final int flushCount = runMerge ? 4 : 7;
            writing = createWriting(new MemDirectory(), 1, 2, 4L,
                    runMerge ? 2 : 8, "balanced");
            for (int key = 0; key < flushCount; key++) {
                writing.put(Integer.valueOf(key), Long.valueOf(key));
            }
        }

        /**
         * Drains the prepared flushes and retains the returned handle for
         * teardown.
         *
         * @return finalized handle
         */
        public SenkuReady<Integer, Long> finish() {
            ready = writing.finishWriting();
            writing = null;
            return ready;
        }

        /**
         * Releases the ready handle after JMH consumes the result.
         */
        @TearDown(Level.Invocation)
        public void tearDown() {
            close(ready);
            ready = null;
        }
    }

    /**
     * Stable finalized index used to measure sorted streaming only.
     */
    @State(Scope.Benchmark)
    public static class ReadyState {

        @Param({ "4096" })
        int entryCount = 4_096;

        private SenkuReady<Integer, Long> ready;

        /**
         * Creates data crossing both page and physical-part boundaries.
         */
        @Setup(Level.Trial)
        public void setup() {
            final SenkuWriting<Integer, Long> writing = createWriting(
                    new MemDirectory(), entryCount + 1, 17, 34L, 64,
                    "balanced");
            for (int key = entryCount - 1; key >= 0; key--) {
                writing.put(Integer.valueOf(key), Long.valueOf(key));
            }
            ready = writing.finishWriting();
        }

        /**
         * Reads and closes one complete stream.
         *
         * @return number of entries
         */
        public long stream() {
            try (Stream<Entry<Integer, Long>> stream = ready.openStream()) {
                return stream.count();
            }
        }

        /**
         * Releases the exclusive ready handle.
         */
        @TearDown(Level.Trial)
        public void tearDown() {
            close(ready);
            ready = null;
        }
    }

    /**
     * Parameterized end-to-end state covering duplicates, shard skew, pages,
     * and physical parts.
     */
    @State(Scope.Thread)
    public static class EndToEndState {

        @Param({ "256" })
        int entryCount = 256;

        @Param({ "0", "50" })
        int duplicatePercent;

        @Param({ "balanced", "skewed" })
        String shardDistribution = "balanced";

        /**
         * Builds, finalizes, and reads only the first globally sorted entry.
         *
         * @return first entry
         */
        public Entry<Integer, Long> run() {
            final MemDirectory directory = new MemDirectory();
            final SenkuWriting<Integer, Long> writing = createWriting(directory,
                    entryCount + 1, 17, 34L, 64, shardDistribution);
            final int uniqueKeys = Math.max(1,
                    entryCount * (100 - duplicatePercent) / 100);
            for (int sequence = entryCount - 1; sequence >= 0; sequence--) {
                final int key = sequence % uniqueKeys;
                writing.put(Integer.valueOf(key), Long.valueOf(sequence));
            }
            try (SenkuReady<Integer, Long> ready = writing.finishWriting();
                    Stream<Entry<Integer, Long>> stream = ready.openStream()) {
                return stream.findFirst().orElseThrow();
            }
        }
    }
}
