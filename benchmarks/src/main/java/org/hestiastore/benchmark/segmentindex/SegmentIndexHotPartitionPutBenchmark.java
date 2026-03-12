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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmark focused on the hot-partition write path.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class SegmentIndexHotPartitionPutBenchmark {

    private static final TypeDescriptorInteger KEY_DESCRIPTOR = new TypeDescriptorInteger();
    private static final TypeDescriptorShortString VALUE_DESCRIPTOR = new TypeDescriptorShortString();
    private static final int HOT_KEY_SPACE = 256;

    private SegmentIndex<Integer, String> index;
    private AtomicInteger sequence;

    @Setup(Level.Trial)
    public void setup() {
        sequence = new AtomicInteger();
        index = SegmentIndex.create(new MemDirectory(), buildConfiguration());
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (index != null) {
            index.close();
            index = null;
        }
    }

    @Benchmark
    @Threads(20)
    public void putHotPartition() {
        final int next = sequence.getAndIncrement();
        final int key = next % HOT_KEY_SPACE;
        index.put(Integer.valueOf(key), buildValue(next));
    }

    @Benchmark
    @Threads(20)
    public void putThenGetHotPartition(final Blackhole blackhole) {
        final int next = sequence.getAndIncrement();
        final int key = next % HOT_KEY_SPACE;
        final String value = buildValue(next);
        index.put(Integer.valueOf(key), value);
        blackhole.consume(index.get(Integer.valueOf(key)));
    }

    private IndexConfiguration<Integer, String> buildConfiguration() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(KEY_DESCRIPTOR)//
                .withValueTypeDescriptor(VALUE_DESCRIPTOR)//
                .withName("segment-index-hot-partition-put-benchmark")//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(512)//
                .withMaxNumberOfKeysInActivePartition(128)//
                .withMaxNumberOfImmutableRunsPerPartition(2)//
                .withMaxNumberOfKeysInPartitionBuffer(512)//
                .withMaxNumberOfKeysInIndexBuffer(4096)//
                .withMaxNumberOfKeysInPartitionBeforeSplit(1_000_000)//
                .withMaxNumberOfKeysInSegmentChunk(64)//
                .withMaxNumberOfSegmentsInCache(8)//
                .withMaxNumberOfDeltaCacheFiles(2)//
                .withBloomFilterIndexSizeInBytes(4096)//
                .withBloomFilterNumberOfHashFunctions(2)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(8 * 1024)//
                .withIndexWorkerThreadCount(4)//
                .withNumberOfStableSegmentMaintenanceThreads(1)//
                .withNumberOfIndexMaintenanceThreads(2)//
                .withNumberOfRegistryLifecycleThreads(1)//
                .withBackgroundMaintenanceAutoEnabled(false)//
                .build();
    }

    private String buildValue(final int next) {
        return "value-" + next;
    }
}
