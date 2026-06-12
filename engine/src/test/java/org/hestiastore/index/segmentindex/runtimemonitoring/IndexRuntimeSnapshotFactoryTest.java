package org.hestiastore.index.segmentindex.runtimemonitoring;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstorecache.ChunkStoreCache;
import org.hestiastore.index.chunkstorecache.ChunkStoreCacheStats;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.core.SegmentIndexStateMachine;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningState;
import org.hestiastore.index.segmentindex.configuration.user.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStatsRecorder;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStatsRecorder;
import org.hestiastore.index.segmentindex.core.split.SplitStats;
import org.hestiastore.index.segmentindex.core.split.SplitStatsView;
import org.hestiastore.index.segmentindex.runtimemonitoring.model.IndexRuntimeSnapshot;
import org.hestiastore.index.segmentindex.wal.WalMonitoring;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class IndexRuntimeSnapshotFactoryTest {

    private ExecutorRegistry executorRegistry;

    @AfterEach
    void tearDown() {
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void createBuildsSnapshotFromCollectedRuntimeInputs() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        final IndexOperationStatsRecorder operationStatsRecorder =
                new IndexOperationStatsRecorder();
        final MaintenanceStatsRecorder maintenanceStatsRecorder =
                new MaintenanceStatsRecorder();
        operationStatsRecorder.recordGetRequest();
        operationStatsRecorder.recordPutRequest();
        executorRegistry = ExecutorRegistryFixture.from(conf);
        final SplitStatsView splitStatsView = mock(SplitStatsView.class);
        @SuppressWarnings("unchecked")
        final ChunkStoreCache<Integer, String> chunkStoreCache = mock(
                ChunkStoreCache.class);
        when(splitStatsView.statsSnapshot())
                .thenReturn(new SplitStats(3L, 2, 0, 0L, 0L));
        when(chunkStoreCache.stats())
                .thenReturn(new ChunkStoreCacheStats(5, 2, 4L, 6L, 7L, 8L,
                        9L, 10L));
        final Instant capturedAt = Instant.parse("2026-06-10T08:15:30Z");
        final IndexRuntimeSnapshotFactory<Integer, String> factory =
                new IndexRuntimeSnapshotFactory<>(effective(conf),
                        splitStatsView, chunkStoreCache,
                        RuntimeTuningState.fromConfiguration(effective(conf)),
                        operationStatsRecorder, new AtomicLong(17L),
                        readyStateView());
        final StableSegmentRuntimeMetrics stableSegmentRuntime =
                new StableSegmentRuntimeMetrics();
        stableSegmentRuntime.setTotalMappedStableSegmentCount(1);
        stableSegmentRuntime.incrementReadyStableSegmentCount();
        stableSegmentRuntime.addSegmentRuntimeSnapshot(
                new SegmentRuntimeSnapshot(SegmentId.of(1),
                        SegmentState.READY, 0L, 8L, 0L, 5L, 2, 3, 5L, 7L, 9L,
                        1L, 8L, 1L));

        final IndexRuntimeSnapshot snapshot = factory.create(capturedAt,
                new SegmentRegistryCacheStats(11L, 12L, 13L, 14L, 2, 9),
                stableSegmentRuntime, executorRegistry.statsSnapshot(),
                WalMonitoring.empty(),
                maintenanceStatsRecorder.statsSnapshot(),
                5L, 7L);

        assertEquals(11L, snapshot.registryCache().hitCount());
        assertEquals(5, snapshot.chunkStoreCache().pageLimit());
        assertEquals(6L, snapshot.chunkStoreCache().hitCount());
        assertEquals(1, snapshot.segments().count());
        assertEquals(5L, snapshot.maintenance().compactRequestCount());
        assertEquals(7L, snapshot.maintenance().flushRequestCount());
        assertEquals(17L, snapshot.wal().appliedLsn());
        assertEquals(capturedAt, snapshot.capturedAt());
        assertFalse(snapshot.wal().enabled());
    }

    private static SegmentIndexStateMachine readyStateView() {
        final SegmentIndexStateMachine stateView = new SegmentIndexStateMachine();
        stateView.markReady();
        return stateView;
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("runtime-snapshot-factory"))
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
