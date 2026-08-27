package org.hestiastore.benchmark.senku;

import static org.hestiastore.index.datatype.NullValue.NULL;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.hestiastore.benchmark.BenchmarkFileSupport;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.FsDirectory;
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
 * Measures eight-thread Senku ingestion with biased bit-board keys and
 * overlapping filesystem-backed flushes.
 */
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 3, jvmArgsAppend = { "-Xms4g", "-Xmx4g" })
@Threads(8)
public class SenkuParallelIngestionBenchmark {

    /**
     * Measures aggregate ingestion throughput including rotations and
     * overlapping flush I/O.
     *
     * @param state       filesystem-backed index
     * @param threadState disjoint biased key sequence
     */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void filesystemIngestThroughput(final IngestionState state,
            final IngestionThreadState threadState) {
        state.put(threadState.nextKey());
    }

    /**
     * Samples caller-visible put latency, including rotation and backpressure
     * outliers.
     *
     * @param state       filesystem-backed index
     * @param threadState disjoint biased key sequence
     */
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void filesystemPutLatency(final IngestionState state,
            final IngestionThreadState threadState) {
        state.put(threadState.nextKey());
    }

    /**
     * Shared filesystem-backed index for one benchmark fork.
     */
    @State(Scope.Benchmark)
    public static class IngestionState {

        @Param({ "10000000" })
        int rotationThreshold = 10_000_000;

        private File tempDirectory;
        private SenkuWriting<Long, NullValue> writing;

        /**
         * Creates one fresh index per fork. Warmup and measurement therefore
         * exercise sustained ingestion with accumulated flush and maintenance
         * work.
         *
         * @throws IOException when the temporary directory cannot be created
         */
        @Setup(Level.Trial)
        public void setup() throws IOException {
            tempDirectory = BenchmarkFileSupport
                    .createTempDir("senku-parallel-ingestion-");
            final SenkuMergeFunctionRegistry<Long, NullValue> functions =
                    new SenkuMergeFunctionRegistry<>();
            functions.register((key, first, second) -> first);
            writing = SenkuIndex
                    .builder(new FsDirectory(tempDirectory),
                            new TypeDescriptorLong(), new TypeDescriptorNull(),
                            functions)
                    .shardHashFunction(key -> key.hashCode()).shardCount(32)
                    .maxInMemoryEntries(rotationThreshold)
                    .maxKeysPerPage(1_000_000).mergeFanIn(64)
                    .maintenanceThreads(8).maintenanceQueueSize(120)
                    .diskIoBufferSize(8_192)
                    .maxEntriesPerPart(10_000_000L).create();
        }

        /**
         * Adds one precomputed non-null key and the non-null singleton value.
         *
         * @param key unique key assigned to the calling benchmark thread
         */
        public void put(final long key) {
            writing.put(Long.valueOf(key), NULL);
        }

        /**
         * Finalizes the index and removes its temporary files.
         */
        @TearDown(Level.Trial)
        public void tearDown() {
            if (writing != null) {
                final SenkuReady<Long, NullValue> ready = writing
                        .finishWriting();
                ready.close();
                writing = null;
            }
            BenchmarkFileSupport.deleteRecursively(tempDirectory);
            tempDirectory = null;
        }
    }

    /**
     * Per-thread disjoint sequence for biased bit-board keys.
     */
    @State(Scope.Thread)
    public static class IngestionThreadState {

        private long sequence;
        private long stride = 1L;

        /**
         * Assigns each worker a disjoint range and a different odd stride. Each
         * worker therefore visits every low bucket-bit combination without
         * correlating thread identity with a fixed subset of stripes.
         *
         * @param threadParams JMH thread identity
         */
        @Setup(Level.Trial)
        public void setup(final ThreadParams threadParams) {
            final int threadIndex = threadParams.getThreadIndex();
            sequence = ((long) threadIndex << 32) | threadIndex;
            stride = 2L * threadIndex + 1L;
        }

        /**
         * Returns a unique key whose JDK-spread low bucket bits are uniform.
         * The old mutation selector conditions each map on those exact bits;
         * avalanche routing distributes them independently.
         *
         * @return next biased key
         */
        long nextKey() {
            final long key = biasedBoardKey(sequence);
            sequence += stride;
            return key;
        }
    }

    /**
     * Encodes a deterministic occupancy-bit pattern whose original JDK
     * {@code HashMap} spread has uniform low bucket bits but vacant hash bits
     * 16 through 20. Higher cycles alter the object value without changing that
     * bit-board-style 32-bit hash.
     *
     * @param sequence non-negative unique sequence
     * @return unique biased bit-board key
     */
    static long biasedBoardKey(final long sequence) {
        final long pattern = sequence & 0x07ff_ffffL;
        final int lowFive = (int) pattern & 31;
        final int hash = lowFive | (int) (pattern >>> 5 & 0x7ff) << 5
                | (int) (pattern >>> 16) << 21;
        final int cycle = (int) (sequence >>> 27);
        final int lowWord = hash ^ cycle;
        return (long) cycle << 32 | Integer.toUnsignedLong(lowWord);
    }
}
