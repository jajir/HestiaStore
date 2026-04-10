package org.hestiastore.benchmark.segmentindex;

import java.util.concurrent.TimeUnit;

import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
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
public class SegmentIndexMultiSegmentGetBenchmark
        extends AbstractSegmentIndexGetBenchmark {

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

    @Override
    protected String tempDirPrefix() {
        return "hestia-jmh-multiget";
    }

    @Override
    protected IndexConfiguration<Integer, String> buildConfiguration() {
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
                .withNumberOfSegmentMaintenanceThreads(1)//
                .withNumberOfIndexMaintenanceThreads(2)//
                .withNumberOfRegistryLifecycleThreads(1)//
                .withBackgroundMaintenanceAutoEnabled(true);
        SegmentIndexBenchmarkSupport.addIntegrityAndCompressionFilters(builder,
                snappy);
        return builder.build();
    }

    @Override
    protected void populateIndex(final SegmentIndex<Integer, String> created) {
        final int flushBatchSize = Math.max(512, maxKeysInSegment / 2);
        SegmentIndexBenchmarkSupport.populateSequential(created, keyCount,
                flushBatchSize, this::buildValue);
    }

    @Override
    protected void afterCreate(final SegmentIndex<Integer, String> created) {
        SegmentIndexBenchmarkSupport.awaitCondition(
                () -> created.metricsSnapshot().getSegmentCount() > 1, 15_000L,
                "Expected persisted multi-segment benchmark layout.");
        created.flushAndWait();
    }

    @Override
    protected void configureReadState(
            final SegmentIndex<Integer, String> openedIndex) {
        if (openedIndex.metricsSnapshot().getSegmentCount() <= 1) {
            throw new IllegalStateException(
                    "Expected reopened multi-segment benchmark layout.");
        }
        setReadKeyBounds(resolveQueryKeyBound(), keyCount * 4);
    }

    @Override
    protected int nextHitKey(final QueryState queryState,
            final int boundExclusive) {
        return queryState.nextRandom(boundExclusive);
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
}
