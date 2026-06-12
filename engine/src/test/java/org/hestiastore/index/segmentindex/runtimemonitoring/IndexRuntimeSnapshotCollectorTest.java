package org.hestiastore.index.segmentindex.runtimemonitoring;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstorecache.LruChunkStoreCache;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateView;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.core.split.SplitStats;
import org.hestiastore.index.segmentindex.core.split.SplitStatsView;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.IndexRuntimeSnapshot;
import org.hestiastore.index.segmentindex.wal.WalMonitoringView;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IndexRuntimeSnapshotCollectorTest {

    private static final String CONF_PROPERTY_MESSAGE =
            "Property 'conf' must not be null.";
    private static final Instant CAPTURED_AT =
            Instant.parse("2026-06-10T08:15:30Z");

    @Mock
    private KeyToSegmentMap<Integer> keyToSegmentMap;

    @Mock
    private SegmentRegistry<Integer, String> segmentRegistry;

    @Mock
    private SegmentRegistry.Runtime<Integer, String> segmentRegistryRuntime;

    @Mock
    private BlockingSegment<Integer, String> segmentHandle;

    @Mock
    private BlockingSegment.Runtime segmentRuntime;

    @Mock
    private Clock clock;

    private IndexOperationStatsRecorder operationStatsRecorder;
    private MaintenanceStatsRecorder maintenanceStatsRecorder;
    private AtomicLong compactRequestHighWaterMark;
    private AtomicLong flushRequestHighWaterMark;
    private AtomicLong lastAppliedWalLsn;
    private ExecutorRegistry executorRegistry;
    private IndexRuntimeSnapshotCollector<Integer, String> collector;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        operationStatsRecorder = new IndexOperationStatsRecorder();
        maintenanceStatsRecorder = new MaintenanceStatsRecorder();
        compactRequestHighWaterMark = new AtomicLong();
        flushRequestHighWaterMark = new AtomicLong();
        lastAppliedWalLsn = new AtomicLong(123L);
        executorRegistry = ExecutorRegistryFixture.from(conf);
        collector = IndexRuntimeSnapshotCollector.create(
                effective(conf), keyToSegmentMap, segmentRegistry,
                () -> new SplitStats(4L, 3, 0, 0L, 0L), executorRegistry,
                RuntimeTuningState.fromConfiguration(effective(conf)),
                new LruChunkStoreCache<>(0), WalMonitoringView.empty(),
                operationStatsRecorder, maintenanceStatsRecorder,
                compactRequestHighWaterMark, flushRequestHighWaterMark,
                lastAppliedWalLsn, readyStateView(), clock);
    }

    @AfterEach
    void tearDown() {
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void runtimeSnapshotKeepsHighWaterMarksWhenRuntimeCountsDrop() {
        when(clock.instant()).thenReturn(CAPTURED_AT);
        when(segmentRegistry.runtime()).thenReturn(segmentRegistryRuntime);
        when(segmentRegistry.metricsSnapshot())
                .thenReturn(new SegmentRegistryCacheStats(11L, 12L, 13L, 14L, 2,
                        9));
        when(keyToSegmentMap.getSegmentIds()).thenReturn(List.of(SegmentId.of(1)));
        when(segmentRegistryRuntime.loadedSegmentsSnapshot())
                .thenReturn(List.of(segmentHandle)).thenReturn(List.of());
        when(segmentHandle.getRuntime()).thenReturn(segmentRuntime);
        when(segmentRuntime.getRuntimeSnapshot()).thenReturn(
                new SegmentRuntimeSnapshot(SegmentId.of(1), SegmentState.READY,
                        0L, 8L, 0L, 5L, 2, 3, 5L, 7L, 9L, 1L, 8L, 1L));

        operationStatsRecorder.recordGetRequest();
        operationStatsRecorder.recordPutRequest();
        operationStatsRecorder.recordDeleteRequest();
        maintenanceStatsRecorder.recordFlushRequest();
        maintenanceStatsRecorder.recordFlushBusyRetry();
        maintenanceStatsRecorder.recordCompactBusyRetry();

        final IndexRuntimeSnapshot firstSnapshot =
                collector.snapshot();
        final IndexRuntimeSnapshot secondSnapshot =
                collector.snapshot();

        assertEquals(1L, firstSnapshot.operations().readOperationCount());
        assertEquals(1L, firstSnapshot.operations().putOperationCount());
        assertEquals(1L, firstSnapshot.operations().deleteOperationCount());
        assertEquals(11L, firstSnapshot.registryCache().hitCount());
        assertEquals(1, firstSnapshot.segments().count());
        assertEquals(1, firstSnapshot.segments().readyCount());
        assertEquals(0, firstSnapshot.segments().maintenanceCount());
        assertEquals(8L, firstSnapshot.segments().totalKeys());
        assertEquals(5L, firstSnapshot.segments().totalCacheKeys());
        assertEquals(2L, firstSnapshot.writePath().totalBufferedWriteKeys());
        assertEquals(3L, firstSnapshot.segments().totalDeltaCacheFiles());
        assertEquals(5L, firstSnapshot.maintenance().compactRequestCount());
        assertEquals(7L, firstSnapshot.maintenance().flushRequestCount());
        assertEquals(123L, firstSnapshot.wal().appliedLsn());
        assertEquals(CAPTURED_AT, firstSnapshot.capturedAt());
        assertFalse(firstSnapshot.wal().enabled());
        assertEquals(4L, firstSnapshot.split().scheduleCount());
        assertEquals(3, firstSnapshot.split().inFlightCount());
        assertEquals(SegmentIndexState.READY, firstSnapshot.state());
        assertEquals(1, firstSnapshot.segments().runtimeMetrics().size());
        assertNotNull(firstSnapshot.segments().runtimeMetrics().get(0));

        assertEquals(5L, secondSnapshot.maintenance().compactRequestCount());
        assertEquals(7L, secondSnapshot.maintenance().flushRequestCount());
        assertEquals(1, secondSnapshot.segments().count());
        assertEquals(5L, compactRequestHighWaterMark.get());
        assertEquals(7L, flushRequestHighWaterMark.get());
        final InOrder collectionOrder = inOrder(clock, keyToSegmentMap);
        collectionOrder.verify(clock).instant();
        collectionOrder.verify(keyToSegmentMap).getSegmentIds();
    }

    @Test
    void createRejectsNullConfiguration() {
        final SplitStatsView splitStatsView =
                () -> new SplitStats(0L, 0, 0, 0L, 0L);
        final RuntimeTuningState runtimeTuningState =
                mock(RuntimeTuningState.class);
        final LruChunkStoreCache<Integer, String> chunkStoreCache =
                new LruChunkStoreCache<>(0);
        final WalMonitoringView walMonitoringView = WalMonitoringView.empty();
        final SegmentIndexStateView stateView = readyStateView();

        final IllegalArgumentException ex = org.junit.jupiter.api.Assertions
                .assertThrows(IllegalArgumentException.class,
                        () -> IndexRuntimeSnapshotCollector.create(null,
                                keyToSegmentMap, segmentRegistry,
                                splitStatsView, executorRegistry,
                                runtimeTuningState, chunkStoreCache,
                                walMonitoringView,
                                operationStatsRecorder,
                                maintenanceStatsRecorder,
                                compactRequestHighWaterMark,
                                flushRequestHighWaterMark, lastAppliedWalLsn,
                                stateView));

        assertEquals(CONF_PROPERTY_MESSAGE, ex.getMessage());
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("metrics-collector"))
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

    private static SegmentIndexStateMachine readyStateView() {
        final SegmentIndexStateMachine stateView = new SegmentIndexStateMachine();
        stateView.markReady();
        return stateView;
    }
}
