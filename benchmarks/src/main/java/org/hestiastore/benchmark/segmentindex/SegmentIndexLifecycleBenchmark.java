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
 * Measures index open and explicit maintenance operations on a persisted
 * layout.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class SegmentIndexLifecycleBenchmark {

    @Param({ "16384" })
    private int keyCount;

    @Param({ "64" })
    private int valueLength;

    @Param({ "false", "true" })
    private boolean snappy;

    @Param({ "off", "sync" })
    private String walMode;

    private File baselineDir;
    private IndexConfiguration<Integer, String> conf;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        baselineDir = SegmentIndexBenchmarkSupport
                .createTempDir("hestia-jmh-lifecycle-base");
        conf = buildConfiguration();
        try (SegmentIndex<Integer, String> created = SegmentIndex
                .create(new FsDirectory(baselineDir), conf)) {
            populateIndex(created);
            created.flushAndWait();
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (baselineDir != null) {
            SegmentIndexBenchmarkSupport.deleteRecursively(baselineDir);
            baselineDir = null;
        }
        conf = null;
    }

    @Benchmark
    @Threads(1)
    public int openExisting() throws IOException {
        try (WorkingCopy workingCopy = openWorkingCopy()) {
            return workingCopy.index.metricsSnapshot().getSegmentCount();
        }
    }

    @Benchmark
    @Threads(1)
    public int openAndCheckAndRepairConsistency() throws IOException {
        try (WorkingCopy workingCopy = openWorkingCopy()) {
            workingCopy.index.checkAndRepairConsistency();
            return workingCopy.index.metricsSnapshot().getSegmentCount();
        }
    }

    @Benchmark
    @Threads(1)
    public int openAndCompact() throws IOException {
        try (WorkingCopy workingCopy = openWorkingCopy()) {
            workingCopy.index.compactAndWait();
            return workingCopy.index.metricsSnapshot().getSegmentCount();
        }
    }

    private WorkingCopy openWorkingCopy() throws IOException {
        final File workingDir = SegmentIndexBenchmarkSupport
                .createTempDir("hestia-jmh-lifecycle-run");
        try {
            SegmentIndexBenchmarkSupport.copyDirectory(baselineDir.toPath(),
                    workingDir.toPath());
            final SegmentIndex<Integer, String> workingIndex = SegmentIndex
                    .open(new FsDirectory(workingDir), conf);
            return new WorkingCopy(workingDir, workingIndex);
        } catch (final IOException | RuntimeException e) {
            SegmentIndexBenchmarkSupport.deleteRecursively(workingDir);
            throw e;
        }
    }

    private IndexConfiguration<Integer, String> buildConfiguration() {
        final int maxKeysBeforeSplit = Math.max(65_536, keyCount * 4);
        final var builder = SegmentIndexBenchmarkSupport
                .baseBuilder("segment-index-lifecycle-benchmark")//
                .withWal(resolveWal())//
                .withMaxNumberOfKeysInSegmentCache(16)//
                .withMaxNumberOfKeysInActivePartition(256)//
                .withMaxNumberOfImmutableRunsPerPartition(2)//
                .withMaxNumberOfKeysInPartitionBuffer(512)//
                .withMaxNumberOfKeysInIndexBuffer(4096)//
                .withMaxNumberOfKeysInSegmentChunk(64)//
                .withMaxNumberOfKeysInSegment(512)//
                .withMaxNumberOfKeysInPartitionBeforeSplit(maxKeysBeforeSplit)//
                .withMaxNumberOfSegmentsInCache(4)//
                .withMaxNumberOfDeltaCacheFiles(2)//
                .withBloomFilterIndexSizeInBytes(Math.max(16_384, keyCount))//
                .withBloomFilterNumberOfHashFunctions(3)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(8 * 1024)//
                .withIndexWorkerThreadCount(4)//
                .withNumberOfStableSegmentMaintenanceThreads(1)//
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

    private void populateIndex(final SegmentIndex<Integer, String> created) {
        int pending = 0;
        for (int key = 0; key < keyCount; key++) {
            created.put(Integer.valueOf(key), buildValue(key));
            pending++;
            if (pending >= 256) {
                created.flushAndWait();
                pending = 0;
            }
        }
        if (pending > 0) {
            created.flushAndWait();
        }
        created.compactAndWait();
    }

    private String buildValue(final int key) {
        return SegmentIndexBenchmarkSupport.buildFixedWidthValue("stable-", key,
                valueLength, 'l');
    }

    private static final class WorkingCopy implements AutoCloseable {

        private final File workingDir;
        private final SegmentIndex<Integer, String> index;

        private WorkingCopy(final File workingDir,
                final SegmentIndex<Integer, String> index) {
            this.workingDir = workingDir;
            this.index = index;
        }

        @Override
        public void close() {
            if (index != null) {
                index.close();
            }
            SegmentIndexBenchmarkSupport.deleteRecursively(workingDir);
        }
    }
}
