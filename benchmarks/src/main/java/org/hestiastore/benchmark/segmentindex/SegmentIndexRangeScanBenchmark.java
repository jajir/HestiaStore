package org.hestiastore.benchmark.segmentindex;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.hestiastore.index.Entry;
import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.segment.SegmentIteratorIsolation;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
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
 * Compares a bounded key-range scan with the previous full-stream filtering
 * fallback on a persisted multi-segment index.
 * <p>
 * Both paths intentionally use {@link SegmentIteratorIsolation#FAIL_FAST}.
 * Every invocation verifies the expected result count so maintenance,
 * cache-capacity eviction, or index closing cannot make a truncated iterator
 * look artificially fast.
 * </p>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class SegmentIndexRangeScanBenchmark {

    private static final Comparator<Integer> KEY_COMPARATOR =
            SegmentIndexBenchmarkSupport.KEY_DESCRIPTOR.getComparator();

    @Param({ "32768" })
    private int keyCount;

    @Param({ "128" })
    private int rangeSize;

    @Param({ "1024" })
    private int maxKeysInSegment;

    @Param({ "64" })
    private int valueLength;

    @Param({ "false", "true" })
    private boolean snappy;

    private File tempDir;
    private SegmentIndex<Integer, String> index;
    private int fromInclusive;
    private int toExclusive;

    /**
     * Creates and reopens a persisted multi-segment index for the measured
     * scans.
     *
     * @throws IOException when benchmark storage cannot be created
     */
    @Setup(Level.Trial)
    public void setup() throws IOException {
        if (keyCount <= 0 || maxKeysInSegment <= 0 || rangeSize <= 0
                || rangeSize > keyCount) {
            throw new IllegalArgumentException(
                    "Key, segment, and range sizes must define a valid range.");
        }
        tempDir = SegmentIndexBenchmarkSupport
                .createTempDir("hestia-jmh-range-scan");
        try (SegmentIndex<Integer, String> created = SegmentIndex.create(
                new FsDirectory(tempDir), buildConfiguration(true))) {
            SegmentIndexBenchmarkSupport.putSequential(created, keyCount,
                    this::buildValue);
            SegmentIndexBenchmarkSupport.awaitCondition(() -> {
                final var snapshot = created.runtimeMonitoring().snapshot();
                return snapshot.segments().count() > 1
                        && snapshot.split().inFlightCount() == 0;
            }, 15_000L,
                    "Expected persisted multi-segment range-scan layout.");
        }

        index = SegmentIndex.open(new FsDirectory(tempDir),
                buildConfiguration(false));
        if (index.runtimeMonitoring().snapshot().segments().count() <= 1) {
            throw new IllegalStateException(
                    "Expected reopened multi-segment range-scan layout.");
        }
        fromInclusive = (keyCount - rangeSize) / 2;
        toExclusive = fromInclusive + rangeSize;
    }

    /**
     * Closes the measured index and removes its temporary files.
     */
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

    /**
     * Measures the bounded range-scan API.
     *
     * @return verified number of scanned entries
     */
    @Benchmark
    public long boundedScan() {
        try (Stream<Entry<Integer, String>> stream = index.scan(fromInclusive,
                toExclusive, SegmentIteratorIsolation.FAIL_FAST)) {
            return requireExpectedCount(stream.count());
        }
    }

    /**
     * Measures the existing unbounded sequential-read API.
     *
     * @return verified number of sequentially read entries
     */
    @Benchmark
    public long sequentialRead() {
        try (Stream<Entry<Integer, String>> stream = index
                .getStream(SegmentIteratorIsolation.FAIL_FAST)) {
            return requireSequentialCount(stream.count());
        }
    }

    /**
     * Measures the former fallback of filtering the full ordered stream.
     *
     * @return verified number of scanned entries
     */
    @Benchmark
    public long fullStreamRangeFallback() {
        try (Stream<Entry<Integer, String>> stream = index
                .getStream(SegmentIteratorIsolation.FAIL_FAST)) {
            final long count = stream
                    .dropWhile(entry -> KEY_COMPARATOR.compare(entry.getKey(),
                            fromInclusive) < 0)
                    .takeWhile(entry -> KEY_COMPARATOR.compare(entry.getKey(),
                            toExclusive) < 0)
                    .count();
            return requireExpectedCount(count);
        }
    }

    private IndexConfiguration<Integer, String> buildConfiguration(
            final boolean backgroundAutoEnabled) {
        final var builder = SegmentIndexBenchmarkSupport
                .baseBuilder("segment-index-range-scan-benchmark")//
                .segment(segment -> segment.cacheKeyLimit(32)
                        .chunkKeyLimit(64).maxKeys(maxKeysInSegment)
                        .cachedSegmentLimit(resolveCachedSegmentLimit())
                        .deltaCacheFileLimit(2))//
                .writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(256)
                        .maintenanceWriteCacheKeyLimit(512)
                        .segmentSplitKeyThreshold(maxKeysInSegment))//
                .bloomFilter(bloomFilter -> bloomFilter
                        .indexSizeBytes(Math.max(16_384, keyCount))
                        .hashFunctions(3).falsePositiveProbability(0.01D))//
                .io(io -> io.diskBufferSizeBytes(8 * 1024))//
                .maintenance(maintenance -> maintenance.indexThreads(2)
                        .registryLifecycleThreads(1)
                        .backgroundAutoEnabled(backgroundAutoEnabled));
        SegmentIndexBenchmarkSupport.addIntegrityAndCompressionFilters(builder,
                snappy);
        return builder.build();
    }

    private int resolveCachedSegmentLimit() {
        final int estimatedSegments = (keyCount + maxKeysInSegment - 1)
                / maxKeysInSegment;
        return Math.max(64, estimatedSegments * 4);
    }

    private String buildValue(final int key) {
        return SegmentIndexBenchmarkSupport.buildFixedWidthValue("stable-", key,
                valueLength, 'r');
    }

    private long requireExpectedCount(final long count) {
        if (count != rangeSize) {
            throw new IllegalStateException(
                    "FAIL_FAST range scan was truncated: expected " + rangeSize
                            + " entries but received " + count + '.');
        }
        return count;
    }

    private long requireSequentialCount(final long count) {
        if (count != keyCount) {
            throw new IllegalStateException(
                    "FAIL_FAST sequential read was truncated: expected "
                            + keyCount + " entries but received " + count + '.');
        }
        return count;
    }
}
