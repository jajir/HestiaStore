package org.hestiastore.benchmark.segmentindex;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.hestiastore.index.directory.FsDirectory;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.api.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.configuration.api.WalDurabilityMode;
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
import org.openjdk.jmh.infra.ThreadParams;

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
    private MutationFlushCoordinator flushCoordinator;

    /**
     * Creates the persisted benchmark index and its stable seed data.
     *
     * @throws IOException when the temporary directory cannot be created
     */
    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDir = SegmentIndexBenchmarkSupport
                .createTempDir("hestia-jmh-mutation");
        index = SegmentIndex.create(new FsDirectory(tempDir),
                buildConfiguration(resolveWal()));
        flushCoordinator = new MutationFlushCoordinator();
        seedStableBase(index);
    }

    /**
     * Persists remaining mutations after each measured iteration.
     */
    @TearDown(Level.Iteration)
    public void flushAfterIteration() {
        if (index != null) {
            index.maintenance().flushAndWait();
            flushCoordinator.reset();
        }
    }

    /**
     * Closes the benchmark index and removes its temporary directory.
     */
    @TearDown(Level.Trial)
    public void tearDown() {
        if (index != null) {
            index.close();
            index = null;
        }
        flushCoordinator = null;
        if (tempDir != null) {
            SegmentIndexBenchmarkSupport.deleteRecursively(tempDir);
            tempDir = null;
        }
    }

    /**
     * Persists one put using thread-local key and flush state.
     *
     * @param cursor thread-local mutation cursor
     */
    @Benchmark
    public void putSync(final MutationCursor cursor) {
        final int key = cursor.nextPutKey(seededKeyCount);
        index.put(Integer.valueOf(key), buildValue("put-", key, 'p'));
        flushIfNeeded();
    }

    /**
     * Persists one delete using thread-local key and flush state.
     *
     * @param cursor thread-local mutation cursor
     */
    @Benchmark
    public void deleteSync(final MutationCursor cursor) {
        index.delete(Integer.valueOf(cursor.nextDeleteKey(seededKeyCount)));
        flushIfNeeded();
    }

    private IndexConfiguration<Integer, String> buildConfiguration(
            final IndexWalConfiguration wal) {
        final int maxKeysBeforeSplit = Math.max(65_536, seededKeyCount * 8);
        final int writeCacheKeyLimit = Math.max(8_192, flushBatchSize * 32);
        final var builder = SegmentIndexBenchmarkSupport
                .baseBuilder("segment-index-persisted-mutation-benchmark")//
                .wal(walBuilder -> walBuilder.configuration(wal))//
                .segment(segment -> segment.cacheKeyLimit(32)
                        .chunkKeyLimit(128).maxKeys(maxKeysBeforeSplit)
                        .cachedSegmentLimit(8).deltaCacheFileLimit(2))//
                .writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(writeCacheKeyLimit)
                        .maintenanceWriteCacheKeyLimit(writeCacheKeyLimit * 2)
                        .indexBufferedWriteKeyLimit(writeCacheKeyLimit * 4)
                        .segmentSplitKeyThreshold(maxKeysBeforeSplit))//
                .bloomFilter(bloomFilter -> bloomFilter
                        .indexSizeBytes(Math.max(16_384, seededKeyCount / 2))
                        .hashFunctions(3)
                        .falsePositiveProbability(0.01D))//
                .io(io -> io.diskBufferSizeBytes(8 * 1024))//
                .maintenance(maintenance -> maintenance
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

    private void seedStableBase(final SegmentIndex<Integer, String> seedingIndex) {
        int pending = 0;
        for (int key = 0; key < seededKeyCount; key++) {
            seedingIndex.put(Integer.valueOf(key),
                    buildValue("seed-", key, 's'));
            pending++;
            if (pending >= flushBatchSize) {
                seedingIndex.maintenance().flushAndWait();
                pending = 0;
            }
        }
        if (pending > 0) {
            seedingIndex.maintenance().flushAndWait();
        }
        seedingIndex.maintenance().compactAndWait();
    }

    private void flushIfNeeded() {
        if (!flushCoordinator.recordAndTryStartFlush(flushBatchSize)) {
            return;
        }
        try {
            index.maintenance().flushAndWait();
        } finally {
            flushCoordinator.finishFlush();
        }
    }

    private String buildValue(final String prefix, final int key,
            final char fillChar) {
        return SegmentIndexBenchmarkSupport.buildFixedWidthValue(prefix, key,
                valueLength, fillChar);
    }

    /**
     * Thread-local key selection for concurrent writers.
     */
    @State(Scope.Thread)
    public static class MutationCursor {

        private int threadIndex;
        private int threadCount;
        private int putOffset;
        private int deleteKey;

        /**
         * Resets the cursor for this writer's disjoint key partitions.
         *
         * @param threadParams JMH thread metadata
         */
        @Setup(Level.Iteration)
        public void setup(final ThreadParams threadParams) {
            threadIndex = threadParams.getThreadIndex();
            threadCount = threadParams.getThreadCount();
            putOffset = threadIndex;
            deleteKey = threadIndex;
        }

        /**
         * Returns the next put key in this writer's disjoint sequence.
         *
         * @param seededKeyCount first key above the stable seed range
         * @return next key for this writer
         */
        int nextPutKey(final int seededKeyCount) {
            final int key = seededKeyCount + putOffset;
            putOffset += threadCount;
            return key;
        }

        /**
         * Returns the next seeded key in this writer's partition.
         *
         * @param seededKeyCount number of stable seeded keys
         * @return next key to delete
         */
        int nextDeleteKey(final int seededKeyCount) {
            final int key = deleteKey;
            deleteKey += threadCount;
            if (deleteKey >= seededKeyCount) {
                deleteKey = threadIndex;
            }
            return key;
        }

    }
}
