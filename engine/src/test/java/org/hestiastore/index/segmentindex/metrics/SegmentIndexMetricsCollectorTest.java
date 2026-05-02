package org.hestiastore.index.segmentindex.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndexMetricsSnapshot;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.mapping.KeyToSegmentMap;
import org.hestiastore.index.segmentindex.core.split.SplitMetricsSnapshot;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentIndexMetricsCollectorTest {

    private static final String CONF_PROPERTY_MESSAGE =
            "Property 'conf' must not be null.";

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

    private Stats stats;
    private AtomicLong compactRequestHighWaterMark;
    private AtomicLong flushRequestHighWaterMark;
    private AtomicLong lastAppliedWalLsn;
    private ExecutorRegistry executorRegistry;
    private WalRuntime<Integer, String> walRuntime;
    private SegmentIndexMetricsCollector<Integer, String> collector;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        stats = new Stats();
        compactRequestHighWaterMark = new AtomicLong();
        flushRequestHighWaterMark = new AtomicLong();
        lastAppliedWalLsn = new AtomicLong(123L);
        executorRegistry = ExecutorRegistryFixture.from(conf);
        walRuntime = WalRuntime.open(new MemDirectory(), IndexWalConfiguration.EMPTY, null, null);
        collector = SegmentIndexMetricsCollector.create(
                conf, keyToSegmentMap, segmentRegistry,
                () -> new SplitMetricsSnapshot(4, 3), executorRegistry,
                RuntimeTuningState.fromConfiguration(conf), walRuntime, stats,
                compactRequestHighWaterMark, flushRequestHighWaterMark,
                lastAppliedWalLsn, () -> SegmentIndexState.READY);
    }

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
    void metricsSnapshotKeepsHighWaterMarksWhenRuntimeCountsDrop() {
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

        stats.recordGetRequest();
        stats.recordPutRequest();
        stats.recordDeleteRequest();
        stats.recordFlushRequest();
        stats.recordSplitScheduled();
        stats.recordFlushBusyRetry();
        stats.recordCompactBusyRetry();

        final SegmentIndexMetricsSnapshot firstSnapshot =
                collector.metricsSnapshot();
        final SegmentIndexMetricsSnapshot secondSnapshot =
                collector.metricsSnapshot();

        assertEquals(11L, firstSnapshot.getRegistryCacheHitCount());
        assertEquals(1, firstSnapshot.getSegmentCount());
        assertEquals(1, firstSnapshot.getSegmentReadyCount());
        assertEquals(0, firstSnapshot.getSegmentMaintenanceCount());
        assertEquals(8L, firstSnapshot.getTotalSegmentKeys());
        assertEquals(5L, firstSnapshot.getTotalSegmentCacheKeys());
        assertEquals(2L, firstSnapshot.getTotalBufferedWriteKeys());
        assertEquals(3L, firstSnapshot.getTotalDeltaCacheFiles());
        assertEquals(5L, firstSnapshot.getCompactRequestCount());
        assertEquals(7L, firstSnapshot.getFlushRequestCount());
        assertEquals(0L, firstSnapshot.getPutBusyRetryCount());
        assertEquals(3, firstSnapshot.getLegacyPartitionCompatibilityMetrics().getSplitBlockedPartitionCount());
        assertEquals(123L, firstSnapshot.getWalAppliedLsn());
        assertFalse(firstSnapshot.isWalEnabled());
        assertEquals(SegmentIndexState.READY, firstSnapshot.getState());
        assertEquals(1, firstSnapshot.getSegmentRuntimeSnapshots().size());
        assertNotNull(firstSnapshot.getSegmentRuntimeSnapshots().get(0));

        assertEquals(5L, secondSnapshot.getCompactRequestCount());
        assertEquals(7L, secondSnapshot.getFlushRequestCount());
        assertEquals(1, secondSnapshot.getSegmentCount());
        assertEquals(5L, compactRequestHighWaterMark.get());
        assertEquals(7L, flushRequestHighWaterMark.get());
    }

    @Test
    void createRejectsNullConfiguration() {
        final IllegalArgumentException ex = org.junit.jupiter.api.Assertions
                .assertThrows(IllegalArgumentException.class,
                        () -> SegmentIndexMetricsCollector.create(null,
                                keyToSegmentMap, segmentRegistry,
                                () -> new SplitMetricsSnapshot(0, 0),
                                executorRegistry,
                                mock(RuntimeTuningState.class), walRuntime,
                                stats, compactRequestHighWaterMark,
                                flushRequestHighWaterMark, lastAppliedWalLsn,
                                () -> SegmentIndexState.READY));

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
                .writePath(writePath -> writePath.legacyImmutableRunLimit(4))
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
