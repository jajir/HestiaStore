package org.hestiastore.benchmark.segmentindex;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * Mixed read/write benchmark for the direct-to-segment runtime.
 *
 * <p>
 * `drainOnly` keeps one hot routed range under continuous maintenance
 * pressure. `splitHeavy` keeps growing the routed key space so the background
 * split policy has to keep materializing child ranges while reads and writes
 * continue.
 * </p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class SegmentIndexMixedDrainBenchmark {

    private static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();

    @Param({ "256" })
    private int hotKeySpace;

    @Param({ "drainOnly", "splitHeavy" })
    private String workloadMode;

    private SegmentIndex<Integer, String> index;
    private AtomicInteger writeSequence;
    private AtomicInteger readSequence;
    private int initialStableKeyCount;

    @Setup(Level.Trial)
    public void setup() {
        writeSequence = new AtomicInteger();
        readSequence = new AtomicInteger();
        index = SegmentIndex.create(new MemDirectory(), buildConfiguration());
        preloadStableBase();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (index != null) {
            index.close();
            index = null;
        }
    }

    @Benchmark
    @Group("partitionedIngestMixed")
    @GroupThreads(16)
    public void putWorkload() {
        final int next = writeSequence.getAndIncrement();
        final int key = resolvePutKey(next);
        index.put(Integer.valueOf(key), buildValue(next));
    }

    @Benchmark
    @Group("partitionedIngestMixed")
    @GroupThreads(4)
    public void getWorkload(final Blackhole blackhole) {
        final int next = readSequence.getAndIncrement();
        final int key = resolveGetKey(next);
        blackhole.consume(index.get(Integer.valueOf(key)));
    }

    private void preloadStableBase() {
        initialStableKeyCount = isSplitHeavy() ? hotKeySpace * 4 : hotKeySpace;
        for (int key = 0; key < initialStableKeyCount; key++) {
            index.put(Integer.valueOf(key), buildValue(key));
        }
        index.maintenance().flushAndWait();
        writeSequence.set(initialStableKeyCount);
    }

    private IndexConfiguration<Integer, String> buildConfiguration() {
        return IndexConfiguration.<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class)
                        .valueClass(String.class)
                        .keyTypeDescriptor(KEY_DESCRIPTOR)
                        .valueTypeDescriptor(VALUE_DESCRIPTOR)
                        .name("segment-index-mixed-drain-benchmark"))//
                .logging(logging -> logging.contextEnabled(false))//
                .segment(segment -> segment.cacheKeyLimit(512)
                        .chunkKeyLimit(64).maxKeys(10_000)
                        .cachedSegmentLimit(8).deltaCacheFileLimit(2))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(64)
                        .legacyImmutableRunLimit(2)
                        .maintenanceWriteCacheKeyLimit(192)
                        .indexBufferedWriteKeyLimit(
                                isSplitHeavy() ? 16_384 : 4_096)
                        .segmentSplitKeyThreshold(resolveSplitThreshold()))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(4096)
                        .hashFunctions(2).falsePositiveProbability(0.01D))//
                .io(io -> io.diskBufferSizeBytes(8 * 1024))//
                .maintenance(maintenance -> maintenance.segmentThreads(1)
                        .indexThreads(2).registryLifecycleThreads(1)
                        .backgroundAutoEnabled(true))//
                .build();
    }

    private boolean isSplitHeavy() {
        return "splitHeavy".equals(workloadMode);
    }

    private int resolveSplitThreshold() {
        if (!isSplitHeavy()) {
            return 1_000_000;
        }
        return Math.max(64, hotKeySpace / 2);
    }

    private int resolvePutKey(final int next) {
        if (isSplitHeavy()) {
            return next;
        }
        return next % hotKeySpace;
    }

    private int resolveGetKey(final int next) {
        if (isSplitHeavy()) {
            final int upperExclusive = Math.max(initialStableKeyCount,
                    writeSequence.get() - 1);
            return next % upperExclusive;
        }
        return next % hotKeySpace;
    }

    private String buildValue(final int next) {
        return "value-" + next;
    }
}
