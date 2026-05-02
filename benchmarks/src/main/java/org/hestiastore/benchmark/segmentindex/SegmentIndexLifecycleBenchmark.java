package org.hestiastore.benchmark.segmentindex;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.IndexWalConfiguration;
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
            created.maintenance().flushAndWait();
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
            return workingCopy.index.runtimeMonitoring().snapshot().getMetrics().getSegmentCount();
        }
    }

    @Benchmark
    @Threads(1)
    public int openAndCheckAndRepairConsistency() throws IOException {
        try (WorkingCopy workingCopy = openWorkingCopy()) {
            workingCopy.index.maintenance().checkAndRepairConsistency();
            return workingCopy.index.runtimeMonitoring().snapshot().getMetrics().getSegmentCount();
        }
    }

    @Benchmark
    @Threads(1)
    public int openAndCompact() throws IOException {
        try (WorkingCopy workingCopy = openWorkingCopy()) {
            workingCopy.index.maintenance().compactAndWait();
            return workingCopy.index.runtimeMonitoring().snapshot().getMetrics().getSegmentCount();
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
                .wal(wal -> wal.configuration(resolveWal()))//
                .segment(segment -> segment.cacheKeyLimit(16)
                        .chunkKeyLimit(64).maxKeys(512)
                        .cachedSegmentLimit(4).deltaCacheFileLimit(2))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(256)
                        .legacyImmutableRunLimit(2)
                        .maintenanceWriteCacheKeyLimit(512)
                        .indexBufferedWriteKeyLimit(4096)
                        .segmentSplitKeyThreshold(maxKeysBeforeSplit))//
                .bloomFilter(bloomFilter -> bloomFilter
                        .indexSizeBytes(Math.max(16_384, keyCount))
                        .hashFunctions(3)
                        .falsePositiveProbability(0.01D))//
                .io(io -> io.diskBufferSizeBytes(8 * 1024))//
                .maintenance(maintenance -> maintenance.segmentThreads(1)
                        .indexThreads(1).registryLifecycleThreads(1)
                        .backgroundAutoEnabled(false));
        SegmentIndexBenchmarkSupport.addIntegrityAndCompressionFilters(builder,
                snappy);
        return builder.build();
    }

    private IndexWalConfiguration resolveWal() {
        if ("sync".equals(walMode)) {
            return IndexWalConfiguration.builder().durability(WalDurabilityMode.SYNC)
                    .build();
        }
        return IndexWalConfiguration.EMPTY;
    }

    private void populateIndex(final SegmentIndex<Integer, String> created) {
        int pending = 0;
        for (int key = 0; key < keyCount; key++) {
            created.put(Integer.valueOf(key), buildValue(key));
            pending++;
            if (pending >= 256) {
                created.maintenance().flushAndWait();
                pending = 0;
            }
        }
        if (pending > 0) {
            created.maintenance().flushAndWait();
        }
        created.maintenance().compactAndWait();
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
