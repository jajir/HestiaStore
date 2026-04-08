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
 * End-to-end benchmark for SegmentIndex point lookups against an on-disk index.
 *
 * <p>
 * The benchmark intentionally closes and reopens the index after populating it.
 * It can then either read only the persisted view or add live updates and
 * measure point lookups that resolve from the segment write cache.
 * </p>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class SegmentIndexGetBenchmark extends AbstractSegmentIndexGetBenchmark {

    @Param({ "12000" })
    private int keyCount;

    @Param({ "256" })
    private int maxKeysInChunk;

    @Param({ "64" })
    private int valueLength;

    @Param({ "false", "true" })
    private boolean snappy;

    @Param({ "persisted", "live" })
    private String readPathMode;

    @Override
    protected String tempDirPrefix() {
        return "hestia-jmh-get";
    }

    @Override
    protected IndexConfiguration<Integer, String> buildConfiguration() {
        final var builder = SegmentIndexBenchmarkSupport
                .baseBuilder("segment-index-get-benchmark")//
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
                .withNumberOfStableSegmentMaintenanceThreads(1)//
                .withNumberOfIndexMaintenanceThreads(1)//
                .withNumberOfRegistryLifecycleThreads(1)//
                .withBackgroundMaintenanceAutoEnabled(false);
        SegmentIndexBenchmarkSupport.addIntegrityAndCompressionFilters(builder,
                snappy);
        return builder.build();
    }

    @Override
    protected void populateIndex(final SegmentIndex<Integer, String> created) {
        final int flushBatchSize = Math.max(1024, maxKeysInChunk * 8);
        SegmentIndexBenchmarkSupport.populateSequential(created, keyCount,
                flushBatchSize, this::buildValue);
    }

    @Override
    protected void afterCreate(final SegmentIndex<Integer, String> created) {
        created.compactAndWait();
    }

    @Override
    protected void configureReadState(
            final SegmentIndex<Integer, String> openedIndex) {
        int queryKeyBound = keyCount;
        if ("live".equals(readPathMode)) {
            queryKeyBound = Math.min(keyCount, 1024);
        }
        setReadKeyBounds(queryKeyBound, keyCount * 2);
        if ("live".equals(readPathMode)) {
            populateLiveUpdates(openedIndex, queryKeyBound);
        }
    }

    @Override
    protected int nextHitKey(final QueryState queryState,
            final int boundExclusive) {
        return queryState.nextSequential(boundExclusive);
    }

    private String buildValue(final int key) {
        return SegmentIndexBenchmarkSupport.buildFixedWidthValue("", key,
                valueLength, 'x');
    }

    private String buildLiveValue(final int key) {
        return SegmentIndexBenchmarkSupport.buildFixedWidthValue("live-", key,
                valueLength, 'o');
    }

    private void populateLiveUpdates(
            final SegmentIndex<Integer, String> openedIndex,
            final int queryKeyBound) {
        for (int key = 0; key < queryKeyBound; key++) {
            openedIndex.put(Integer.valueOf(key), buildLiveValue(key));
        }
    }
}
