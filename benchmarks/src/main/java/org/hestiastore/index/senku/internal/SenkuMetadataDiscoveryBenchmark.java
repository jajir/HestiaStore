package org.hestiastore.index.senku.internal;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datatype.NullValue;
import org.hestiastore.index.datatype.TypeDescriptorLong;
import org.hestiastore.index.datatype.TypeDescriptorNull;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.senku.SenkuLongKeySummary;
import org.hestiastore.index.senku.SenkuMergeFunctions;
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
 * Repeated WRITING discovery of immutable committed manifests. The configured
 * fan-in deliberately exceeds each shard's run count, so no merge is scheduled.
 * MemDirectory isolates metadata parsing/allocation from physical disk latency.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 4, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 2, jvmArgsAppend = { "-Xms1g", "-Xmx1g" })
public class SenkuMetadataDiscoveryBenchmark {
    @Param({ "128" })
    int shardCount = 128;

    @Param({ "64" })
    int runsPerShard = 64;

    @Param({ "0", "256" })
    int summarySize = 256;

    private SenkuMaintenanceCoordinator<Long, NullValue> coordinator;
    private ThreadPoolExecutor workers;
    private AtomicReference<IndexException> failure;

    /** Publishes only the metadata examined by coordinator discovery. */
    @Setup(Level.Trial)
    public void setupTrial() {
        if (shardCount < 1 || runsPerShard < 1
                || runsPerShard == Integer.MAX_VALUE || summarySize < 0
                || summarySize > 256) {
            throw new IllegalArgumentException("Invalid discovery fixture");
        }
        final MemDirectory root = new MemDirectory();
        root.mkdir(SenkuFileNames.FLUSH_DIRECTORY);
        final long[] keys = new long[summarySize];
        final long[] weights = new long[summarySize];
        Arrays.fill(weights, 1);
        for (int index = 0; index < keys.length; index++) {
            keys[index] = index * 1_000_003L;
        }
        final SenkuRunManifest manifest = new SenkuRunManifest(
                summarySize == 0 ? 0 : 1, summarySize, Optional.of(
                        SenkuLongKeySummary.of(summarySize, keys, weights)));
        for (int shard = 0; shard < shardCount; shard++) {
            root.mkdir(SenkuFileNames.shardDirectory(shard));
            final Directory shardDirectory = root
                    .openSubDirectory(SenkuFileNames.shardDirectory(shard));
            shardDirectory.mkdir(SenkuFileNames.levelDirectory(0));
            final Directory level = shardDirectory
                    .openSubDirectory(SenkuFileNames.levelDirectory(0));
            for (int run = 0; run < runsPerShard; run++) {
                level.mkdir(SenkuFileNames.runDirectory(run));
                SenkuMetadataCodec.publishRunManifest(level.openSubDirectory(
                        SenkuFileNames.runDirectory(run)), manifest);
            }
        }
        workers = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1));
        failure = new AtomicReference<>();
        coordinator = new SenkuMaintenanceCoordinator<>(root, shardCount,
                runsPerShard + 1, new TypeDescriptorLong(),
                new TypeDescriptorNull(), SenkuMergeFunctions.longSet(),
                1_000_000, 10_000_000L, DataBlockSize.ofDataBlockSize(8192),
                workers, Runnable::run, failure, ignored -> {
                    // No eligible work, hence no live ingestion gate.
                });
        scan();
    }

    /** Measures repeat discovery and verifies complete catalog cardinality. */
    @Benchmark
    public int scan() {
        coordinator.discoverAndScheduleOnce(false);
        if (failure.get() != null) {
            throw failure.get();
        }
        final int count = coordinator.runCount();
        if (count != Math.multiplyExact(shardCount, runsPerShard)
                || coordinator.submittedCount() != 0) {
            throw new IllegalStateException("Incomplete discovery fixture");
        }
        return count;
    }

    /** Releases the benchmark-owned executor without any queued work. */
    @TearDown(Level.Trial)
    public void tearDown() {
        if (workers != null) {
            workers.shutdownNow();
        }
    }
}
