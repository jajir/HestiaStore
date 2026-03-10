package org.hestiastore.benchmark.segmentindex;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkFilterSnappyCompress;
import org.hestiastore.index.chunkstore.ChunkFilterSnappyDecompress;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
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
 * End-to-end benchmark for SegmentIndex point lookups against an on-disk index.
 *
 * <p>
 * The benchmark intentionally closes and reopens the index after populating it.
 * It can then either read only the persisted view or add a live overlay and
 * measure point lookups that resolve from the active partition layer.
 * </p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class SegmentIndexGetBenchmark {

    private static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();
    private static final Comparator<File> REVERSE_FILE_ORDER = Comparator
            .comparing(File::getAbsolutePath).reversed();
    private static final Path JMH_TEMP_ROOT = Path.of("target", "jmh-temp");

    @Param({ "12000" })
    private int keyCount;

    @Param({ "256" })
    private int maxKeysInChunk;

    @Param({ "64" })
    private int valueLength;

    @Param({ "false", "true" })
    private boolean snappy;

    @Param({ "persisted", "overlay" })
    private String readPathMode;

    private File tempDir;
    private SegmentIndex<Integer, String> index;
    private int queryKeyBound;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Files.createDirectories(JMH_TEMP_ROOT);
        tempDir = Files.createTempDirectory(JMH_TEMP_ROOT, "hestia-jmh-get")
                .toFile();
        final Directory directory = new FsDirectory(tempDir);
        final IndexConfiguration<Integer, String> conf = buildConfiguration();

        try (SegmentIndex<Integer, String> created = SegmentIndex.create(
                directory, conf)) {
            populateIndex(created);
            created.compactAndWait();
        }

        index = SegmentIndex.open(new FsDirectory(tempDir), conf);
        queryKeyBound = keyCount;
        if ("overlay".equals(readPathMode)) {
            populateOverlay(index);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (index != null) {
            index.close();
            index = null;
        }
        if (tempDir != null) {
            deleteRecursively(tempDir);
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

    private IndexConfiguration<Integer, String> buildConfiguration() {
        final var builder = IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(KEY_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_DESCRIPTOR)//
                .withName("segment-index-get-benchmark")//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(8)//
                .withMaxNumberOfKeysInActivePartition(2048)//
                .withMaxNumberOfImmutableRunsPerPartition(2)//
                .withMaxNumberOfKeysInPartitionBuffer(4096)//
                .withMaxNumberOfKeysInIndexBuffer(12_288)//
                .withMaxNumberOfKeysInSegmentChunk(maxKeysInChunk)//
                .withMaxNumberOfKeysInSegment(keyCount * 2)//
                .withMaxNumberOfKeysInPartitionBeforeSplit(keyCount * 2)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withMaxNumberOfDeltaCacheFiles(2)//
                .withBloomFilterIndexSizeInBytes(Math.max(8192, keyCount / 2))//
                .withBloomFilterNumberOfHashFunctions(3)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(8 * 1024)//
                .withIndexWorkerThreadCount(4)//
                .withNumberOfSegmentIndexMaintenanceThreads(1)//
                .withNumberOfIndexMaintenanceThreads(1)//
                .withNumberOfRegistryLifecycleThreads(1)//
                .withSegmentMaintenanceAutoEnabled(false);
        builder.addEncodingFilter(new ChunkFilterCrc32Writing())
                .addEncodingFilter(new ChunkFilterMagicNumberWriting());
        builder.addDecodingFilter(new ChunkFilterMagicNumberValidation())
                .addDecodingFilter(new ChunkFilterCrc32Validation());
        if (snappy) {
            builder.addEncodingFilter(new ChunkFilterSnappyCompress());
            builder.withDecodingFilters(java.util.List.of(
                    new ChunkFilterMagicNumberValidation(),
                    new ChunkFilterSnappyDecompress(),
                    new ChunkFilterCrc32Validation()));
        }
        return builder.build();
    }

    private void populateIndex(final SegmentIndex<Integer, String> created) {
        final int flushBatchSize = Math.max(1024, maxKeysInChunk * 8);
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

    private void populateOverlay(final SegmentIndex<Integer, String> openedIndex) {
        queryKeyBound = Math.min(keyCount, 1024);
        for (int key = 0; key < queryKeyBound; key++) {
            openedIndex.put(Integer.valueOf(key), buildOverlayValue(key));
        }
    }

    private String buildValue(final int key) {
        final String prefix = Integer.toString(key) + '-';
        final int suffixLength = Math.max(0, valueLength - prefix.length());
        return prefix + "x".repeat(suffixLength);
    }

    private String buildOverlayValue(final int key) {
        final String prefix = "overlay-" + key + '-';
        final int suffixLength = Math.max(0, valueLength - prefix.length());
        return prefix + "o".repeat(suffixLength);
    }

    private static void deleteRecursively(final File file) {
        if (file == null || !file.exists()) {
            return;
        }
        final File[] children = file.listFiles();
        if (children != null) {
            java.util.Arrays.sort(children, REVERSE_FILE_ORDER);
            for (final File child : children) {
                deleteRecursively(child);
            }
        }
        if (!file.delete()) {
            throw new IllegalStateException(
                    "Unable to delete benchmark temp path: "
                            + file.getAbsolutePath());
        }
    }

    @State(Scope.Thread)
    public static class QueryState {

        private int cursor;

        @Setup(Level.Iteration)
        public void setup() {
            cursor = 0;
        }

        int nextKey(final int boundExclusive) {
            final int key = cursor;
            cursor++;
            if (cursor >= boundExclusive) {
                cursor = 0;
            }
            return key;
        }
    }
}
