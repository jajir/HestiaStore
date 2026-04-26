package org.hestiastore.index.segmentindex.core.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.Wal;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.executor.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.split.SplitMetricsSnapshot;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SegmentIndexMetricsSnapshotsTest {

    private static final String CONF_PROPERTY_MESSAGE =
            "Property 'conf' must not be null.";

    private IndexExecutorRegistry executorRegistry;
    private WalRuntime<Integer, String> walRuntime;

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
    void createReturnsMetricsSnapshotSupplier() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        final KeyToSegmentMap<Integer> keyToSegmentMap = mock(
                KeyToSegmentMap.class);
        final SegmentRegistry<Integer, String> segmentRegistry = mock(
                SegmentRegistry.class);
        final SegmentRegistry.Runtime<Integer, String> runtime = mock(
                SegmentRegistry.Runtime.class);
        executorRegistry = IndexExecutorRegistry.create(conf);
        walRuntime = WalRuntime.open(new MemDirectory(), Wal.EMPTY, null, null);
        Mockito.when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of());
        Mockito.when(segmentRegistry.runtime()).thenReturn(runtime);
        Mockito.when(runtime.loadedSegmentsSnapshot()).thenReturn(List.of());
        Mockito.when(segmentRegistry.metricsSnapshot())
                .thenReturn(new SegmentRegistryCacheStats(0L, 0L, 0L, 0L, 0,
                        0));

        final Supplier<SegmentIndexMetricsSnapshot> snapshotSupplier =
                SegmentIndexMetricsSnapshots.create(conf, keyToSegmentMap,
                        segmentRegistry,
                        () -> new SplitMetricsSnapshot(0, 0),
                        executorRegistry,
                        RuntimeTuningState.fromConfiguration(conf), walRuntime,
                        new Stats(), new AtomicLong(), new AtomicLong(),
                        new AtomicLong(), () -> SegmentIndexState.READY);

        assertNotNull(snapshotSupplier);
        assertEquals(SegmentIndexState.READY,
                snapshotSupplier.get().getState());
    }

    @Test
    void createRejectsNullConfiguration() {
        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> SegmentIndexMetricsSnapshots.create(null,
                        mock(KeyToSegmentMap.class),
                        mock(SegmentRegistry.class),
                        () -> new SplitMetricsSnapshot(0, 0),
                        mock(IndexExecutorRegistry.class),
                        mock(RuntimeTuningState.class),
                        mock(WalRuntime.class), new Stats(), new AtomicLong(),
                        new AtomicLong(), new AtomicLong(),
                        () -> SegmentIndexState.READY));

        assertEquals(CONF_PROPERTY_MESSAGE, ex.getMessage());
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("metrics-snapshots")
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
