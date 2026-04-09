package org.hestiastore.benchmark.segmentindex;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.WalDurabilityMode;
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

/**
 * Persisted mutation benchmark covering put and delete paths with periodic
 * flushes.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class SegmentIndexPersistedMutationBenchmark {

    @Param({ "100000" })
    private int seededKeyCount;

    @Param({ "64" })
    private int valueLength;

    @Param({ "false", "true" })
    private boolean snappy;

    @Param({ "off", "sync" })
    private String walMode;

    @Param({ "256" })
    private int flushBatchSize;

    private File tempDir;
    private SegmentIndex<Integer, String> index;
    private int putSequence;
    private int deleteSequence;
    private int pendingMutationCount;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = SegmentIndexBenchmarkSupport
                .createTempDir("hestia-jmh-mutation");
        seedStableBase();
        index = SegmentIndex.create(new FsDirectory(tempDir),
                buildConfiguration(resolveWal()));
    }

    @Setup(Level.Iteration)
    public void resetIterationState() {
        putSequence = seededKeyCount;
        deleteSequence = 0;
        pendingMutationCount = 0;
    }

    @TearDown(Level.Iteration)
    public void flushAfterIteration() {
        if (index != null && pendingMutationCount > 0) {
            index.flushAndWait();
            pendingMutationCount = 0;
        }
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
    @Threads(1)
    public void putSync() {
        final int key = putSequence++;
        index.put(Integer.valueOf(key), buildValue("put-", key, 'p'));
        flushIfNeeded();
    }

    @Benchmark
    @Threads(1)
    public void deleteSync() {
        index.delete(Integer.valueOf(deleteSequence));
        deleteSequence = advanceDeleteCursor(deleteSequence);
        flushIfNeeded();
    }

    private IndexConfiguration<Integer, String> buildConfiguration(
            final Wal wal) {
        final int maxKeysBeforeSplit = Math.max(65_536, seededKeyCount * 8);
        final var builder = SegmentIndexBenchmarkSupport
                .baseBuilder("segment-index-persisted-mutation-benchmark")//
                .withWal(wal)//
                .withMaxNumberOfKeysInSegmentCache(32)//
                .withMaxNumberOfKeysInActivePartition(512)//
                .withMaxNumberOfImmutableRunsPerPartition(2)//
                .withMaxNumberOfKeysInPartitionBuffer(1024)//
                .withMaxNumberOfKeysInIndexBuffer(8192)//
                .withMaxNumberOfKeysInSegmentChunk(128)//
                .withMaxNumberOfKeysInSegment(maxKeysBeforeSplit)//
                .withMaxNumberOfKeysInPartitionBeforeSplit(maxKeysBeforeSplit)//
                .withMaxNumberOfSegmentsInCache(8)//
                .withMaxNumberOfDeltaCacheFiles(2)//
                .withBloomFilterIndexSizeInBytes(Math.max(16_384,
                        seededKeyCount / 2))//
                .withBloomFilterNumberOfHashFunctions(3)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(8 * 1024)//
                .withNumberOfSegmentMaintenanceThreads(1)//
                .withNumberOfIndexMaintenanceThreads(1)//
                .withNumberOfRegistryLifecycleThreads(1)//
                .withBackgroundMaintenanceAutoEnabled(false);
        SegmentIndexBenchmarkSupport.addIntegrityAndCompressionFilters(builder,
                snappy);
        return builder.build();
    }

    private Wal resolveWal() {
        if ("sync".equals(walMode)) {
            return Wal.builder().withDurabilityMode(WalDurabilityMode.SYNC)
                    .build();
        }
        return Wal.EMPTY;
    }

    private void seedStableBase() {
        try (SegmentIndex<Integer, String> seedingIndex = SegmentIndex.create(
                new FsDirectory(tempDir), buildConfiguration(Wal.EMPTY))) {
            seedStableBase(seedingIndex);
        }
    }

    private void seedStableBase(final SegmentIndex<Integer, String> seedingIndex) {
        int pending = 0;
        for (int key = 0; key < seededKeyCount; key++) {
            seedingIndex.put(Integer.valueOf(key),
                    buildValue("seed-", key, 's'));
            pending++;
            if (pending >= flushBatchSize) {
                seedingIndex.flushAndWait();
                pending = 0;
            }
        }
        if (pending > 0) {
            seedingIndex.flushAndWait();
        }
        seedingIndex.compactAndWait();
    }

    private int advanceDeleteCursor(final int currentKey) {
        if (currentKey + 1 >= seededKeyCount) {
            return 0;
        }
        return currentKey + 1;
    }

    private void flushIfNeeded() {
        pendingMutationCount++;
        if (pendingMutationCount >= flushBatchSize) {
            index.flushAndWait();
            pendingMutationCount = 0;
        }
    }

    private String buildValue(final String prefix, final int key,
            final char fillChar) {
        return SegmentIndexBenchmarkSupport.buildFixedWidthValue(prefix, key,
                valueLength, fillChar);
    }
}
