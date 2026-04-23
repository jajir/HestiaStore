package org.hestiastore.index.segmentindex.core.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.maintenance.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.split.SplitRuntimeSnapshot;
import org.hestiastore.index.segmentindex.core.split.SplitService;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentindex.wal.WalStats;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexMetricsSnapshotFactoryTest {

    @Mock
    private SplitService<Integer, String> splitService;

    private WalRuntime<Integer, String> walRuntime;
    private IndexExecutorRegistry executorRegistry;

    @AfterEach
    void tearDown() {
        if (walRuntime != null) {
            walRuntime.close();
        }
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void createBuildsSnapshotFromCollectedRuntimeInputs() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        final Stats stats = new Stats();
        stats.recordGetRequest();
        stats.recordPutRequest();
        executorRegistry = new IndexExecutorRegistry(conf);
        final SegmentIndexMetricsSnapshotFactory<Integer, String> factory =
                new SegmentIndexMetricsSnapshotFactory<>(conf,
                        splitService,
                        RuntimeTuningState.fromConfiguration(conf),
                        openDisabledWalRuntime(), stats, new AtomicLong(17L),
                        () -> SegmentIndexState.READY);
        when(splitService.runtimeSnapshot())
                .thenReturn(new SplitRuntimeSnapshot(3, 2));
        final StableSegmentRuntimeMetrics stableSegmentRuntime =
                new StableSegmentRuntimeMetrics();
        stableSegmentRuntime.setTotalMappedStableSegmentCount(1);
        stableSegmentRuntime.incrementReadyStableSegmentCount();
        stableSegmentRuntime.addSegmentRuntimeSnapshot(
                new SegmentRuntimeSnapshot(SegmentId.of(1),
                        SegmentState.READY, 0L, 8L, 0L, 5L, 2, 3, 5L, 7L, 9L,
                        1L, 8L, 1L));

        final SegmentIndexMetricsSnapshot snapshot = factory.create(
                new SegmentRegistryCacheStats(11L, 12L, 13L, 14L, 2, 9),
                stableSegmentRuntime, executorRegistry.runtimeSnapshot(),
                new WalStats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0L, 0L, 0L, 0L,
                        0L, 0L, 0L),
                5L, 7L);

        assertEquals(11L, snapshot.getRegistryCacheHitCount());
        assertEquals(1, snapshot.getSegmentCount());
        assertEquals(5L, snapshot.getCompactRequestCount());
        assertEquals(7L, snapshot.getFlushRequestCount());
        assertEquals(17L, snapshot.getWalAppliedLsn());
        assertFalse(snapshot.isWalEnabled());
    }

    private WalRuntime<Integer, String> openDisabledWalRuntime() {
        walRuntime = WalRuntime.open(new MemDirectory(), Wal.EMPTY, null, null);
        return walRuntime;
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("metrics-snapshot-factory")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInActivePartition(5)
                .withMaxNumberOfImmutableRunsPerPartition(4)
                .withMaxNumberOfKeysInPartitionBuffer(6)
                .withMaxNumberOfKeysInIndexBuffer(7)
                .withMaxNumberOfKeysInPartitionBeforeSplit(8)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withBackgroundMaintenanceAutoEnabled(false)
                .withNumberOfIndexMaintenanceThreads(1)
                .withNumberOfSegmentMaintenanceThreads(1)
                .withNumberOfRegistryLifecycleThreads(1)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }
}
