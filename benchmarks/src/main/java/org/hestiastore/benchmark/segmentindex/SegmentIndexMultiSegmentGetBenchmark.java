package org.hestiastore.benchmark.segmentindex;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
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
 * Persisted multi-segment point lookup benchmark with hot and cold working
 * sets.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class SegmentIndexMultiSegmentGetBenchmark {

    @Param({ "32768" })
    private int keyCount;

    @Param({ "1024" })
    private int maxKeysInSegment;

    @Param({ "64" })
    private int valueLength;

    @Param({ "false", "true" })
    private boolean snappy;

    @Param({ "hot", "cold" })
    private String workingSetMode;

    @Param({ "3", "8" })
    private int maxSegmentsInCache;

    private File tempDir;
    private SegmentIndex<Integer, String> index;
    private int queryKeyBound;
    private int missKeyStart;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = SegmentIndexBenchmarkSupport
                .createTempDir("hestia-jmh-multiget");
        final IndexConfiguration<Integer, String> conf = buildConfiguration();

        try (SegmentIndex<Integer, String> created = SegmentIndex
                .create(new FsDirectory(tempDir), conf)) {
            populateIndex(created);
            SegmentIndexBenchmarkSupport.awaitCondition(
                    () -> created.metricsSnapshot().getSegmentCount() > 1,
                    15_000L,
                    "Expected persisted multi-segment benchmark layout.");
            created.flushAndWait();
        }

        index = SegmentIndex.open(new FsDirectory(tempDir), conf);
        if (index.metricsSnapshot().getSegmentCount() <= 1) {
            throw new IllegalStateException(
                    "Expected reopened multi-segment benchmark layout.");
        }
        queryKeyBound = resolveQueryKeyBound();
        missKeyStart = keyCount * 4;
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (index != null) {
            index.close();
            index = null;
        }
        if (tempDir != null) {
            SegmentIndexBenchmarkSupport.deleteRecursively(tempDir);
            tempDir = null;
        }
    }

    @Benchmark
    public String getHitSync(final QueryState queryState) {
        return index.get(Integer.valueOf(queryState.nextKey(queryKeyBound)));
    }

    @Benchmark
    public String getHitAsyncJoin(final QueryState queryState) {
        return index.getAsync(Integer.valueOf(queryState.nextKey(queryKeyBound)))
                .toCompletableFuture().join();
    }

    @Benchmark
    public String getMissSync(final QueryState queryState) {
        return index.get(Integer.valueOf(
                missKeyStart + queryState.nextKey(queryKeyBound)));
    }

    @Benchmark
    public String getMissAsyncJoin(final QueryState queryState) {
        return index
                .getAsync(Integer.valueOf(
                        missKeyStart + queryState.nextKey(queryKeyBound)))
                .toCompletableFuture().join();
    }

    private IndexConfiguration<Integer, String> buildConfiguration() {
        final var builder = SegmentIndexBenchmarkSupport
                .baseBuilder("segment-index-multi-segment-get-benchmark")//
                .withMaxNumberOfKeysInSegmentCache(32)//
                .withMaxNumberOfKeysInActivePartition(256)//
                .withMaxNumberOfImmutableRunsPerPartition(2)//
                .withMaxNumberOfKeysInPartitionBuffer(512)//
                .withMaxNumberOfKeysInIndexBuffer(8_192)//
                .withMaxNumberOfKeysInSegmentChunk(64)//
                .withMaxNumberOfKeysInSegment(maxKeysInSegment)//
                .withMaxNumberOfKeysInPartitionBeforeSplit(maxKeysInSegment)//
                .withMaxNumberOfSegmentsInCache(maxSegmentsInCache)//
                .withMaxNumberOfDeltaCacheFiles(2)//
                .withBloomFilterIndexSizeInBytes(Math.max(16_384, keyCount))//
                .withBloomFilterNumberOfHashFunctions(3)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(8 * 1024)//
                .withIndexWorkerThreadCount(4)//
                .withNumberOfStableSegmentMaintenanceThreads(1)//
                .withNumberOfIndexMaintenanceThreads(2)//
                .withNumberOfRegistryLifecycleThreads(1)//
                .withBackgroundMaintenanceAutoEnabled(true);
        SegmentIndexBenchmarkSupport.addIntegrityAndCompressionFilters(builder,
                snappy);
        return builder.build();
    }

    private void populateIndex(final SegmentIndex<Integer, String> created) {
        final int flushBatchSize = Math.max(512, maxKeysInSegment / 2);
        int pending = 0;
        for (int key = 0; key < keyCount; key++) {
            created.put(Integer.valueOf(key), buildValue(key));
            pending++;
            if (pending >= flushBatchSize) {
                created.flushAndWait();
                pending = 0;
            }
        }
        if (pending > 0) {
            created.flushAndWait();
        }
    }

    private int resolveQueryKeyBound() {
        if ("cold".equals(workingSetMode)) {
            return keyCount;
        }
        return Math.min(keyCount, Math.max(maxKeysInSegment * 4, 1024));
    }

    private String buildValue(final int key) {
        return SegmentIndexBenchmarkSupport.buildFixedWidthValue("stable-", key,
                valueLength, 'm');
    }

    @State(Scope.Thread)
    public static class QueryState {

        private int state;

        @Setup(Level.Iteration)
        public void setup() {
            state = 0x13579BDF;
        }

        int nextKey(final int boundExclusive) {
            state = state * 1664525 + 1013904223;
            return (state & Integer.MAX_VALUE) % boundExclusive;
        }
    }
}
