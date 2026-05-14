package org.hestiastore.index.segmentindex.metrics;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

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
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.user.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStats;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
class SegmentIndexMetricsSnapshotsTest {

    private static final String CONF_PROPERTY_MESSAGE =
            "Property 'conf' must not be null.";

    private ExecutorRegistry executorRegistry;
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
        executorRegistry = ExecutorRegistryFixture.from(conf);
        walRuntime = WalRuntime.open(new MemDirectory(), IndexWalConfiguration.EMPTY, null, null);
        final IndexOperationStatsRecorder operationStatsRecorder =
                new IndexOperationStatsRecorder();
        final MaintenanceStatsRecorder maintenanceStatsRecorder =
                new MaintenanceStatsRecorder();
        Mockito.when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of());
        Mockito.when(segmentRegistry.runtime()).thenReturn(runtime);
        Mockito.when(runtime.loadedSegmentsSnapshot()).thenReturn(List.of());
        Mockito.when(segmentRegistry.metricsSnapshot())
                .thenReturn(new SegmentRegistryCacheStats(0L, 0L, 0L, 0L, 0,
                        0));

        final Supplier<SegmentIndexMetricsSnapshot> snapshotSupplier =
                SegmentIndexMetricsSnapshots.create(effective(conf), keyToSegmentMap,
                        segmentRegistry,
                        () -> new SplitStats(0L, 0, 0, 0L, 0L),
                        executorRegistry,
                        RuntimeTuningState.fromConfiguration(effective(conf)), walRuntime,
                        operationStatsRecorder::statsSnapshot,
                        maintenanceStatsRecorder::statsSnapshot,
                        new AtomicLong(), new AtomicLong(),
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
                        () -> new SplitStats(0L, 0, 0, 0L, 0L),
                        mock(ExecutorRegistry.class),
                        mock(RuntimeTuningState.class),
                        mock(WalRuntime.class),
                        new IndexOperationStatsRecorder()::statsSnapshot,
                        new MaintenanceStatsRecorder()::statsSnapshot,
                        new AtomicLong(),
                        new AtomicLong(), new AtomicLong(),
                        () -> SegmentIndexState.READY));

        assertEquals(CONF_PROPERTY_MESSAGE, ex.getMessage());
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("metrics-snapshots"))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))
                .writePath(writePath -> writePath.indexBufferedWriteKeyLimit(7))
                .writePath(writePath -> writePath.segmentSplitKeyThreshold(8))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .maintenance(maintenance -> maintenance.backgroundAutoEnabled(false))
                .maintenance(maintenance -> maintenance.indexThreads(1))
                .maintenance(maintenance -> maintenance.segmentThreads(1))
                .maintenance(maintenance -> maintenance.registryLifecycleThreads(1))
                .filters(filters -> filters.encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters.decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }
}
