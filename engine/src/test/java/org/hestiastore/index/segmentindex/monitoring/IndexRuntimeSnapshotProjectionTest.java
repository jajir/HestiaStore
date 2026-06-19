package org.hestiastore.index.segmentindex.monitoring;

import static org.hestiastore.index.segmentindex.configuration.effective.EffectiveIndexConfigurationTestSupport.effective;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.time.Instant;
import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.chunkstorecache.ChunkStoreCacheStats;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentRuntimeSnapshot;
import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.execution.MaintenanceStatsSnapshot;
import org.hestiastore.index.segmentindex.core.execution.OperationStatsSnapshot;
import org.hestiastore.index.segmentindex.core.split.SplitStats;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexRuntimeSnapshot;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexSegmentRuntimeMetrics;
import org.hestiastore.index.segmentindex.wal.WalMonitoring;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class IndexRuntimeSnapshotProjectionTest {

    private ExecutorRegistry executorRegistry;

    @AfterEach
    void tearDown() {
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void projectBuildsSnapshotFromCollectedRuntimeInputs() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        executorRegistry = ExecutorRegistryFixture.from(conf);
        final Instant capturedAt = Instant.parse("2026-06-10T08:15:30Z");
        final IndexRuntimeSnapshotProjection<Integer, String> projection =
                new IndexRuntimeSnapshotProjection<>(effective(conf));
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentRuntimeMetrics stableSegmentRuntime =
                new SegmentRuntimeMetrics();
        stableSegmentRuntime.setTotalMappedStableSegmentCount(1);
        stableSegmentRuntime.incrementReadyStableSegmentCount();
        stableSegmentRuntime.addSegmentRuntimeSnapshot(
                new SegmentRuntimeSnapshot(segmentId, SegmentState.READY, 0L,
                        8L, 0L, 5L, 2, 3, 5L, 7L, 9L, 1L, 8L, 1L));
        final RuntimeMonitoringData collected =
                new RuntimeMonitoringData(capturedAt,
                        new OperationStatsSnapshot(1L, 1L, 0L, 0L, 0L, 0L, 0L, 0L,
                                0L),
                        new SegmentRegistryCacheStats(11L, 12L, 13L, 14L, 2,
                                9),
                        new ChunkStoreCacheStats(5, 2, 4L, 6L, 7L, 8L, 9L,
                                10L),
                        stableSegmentRuntime, executorRegistry.statsSnapshot(),
                        new SplitStats(3L, 2, 0, 0L, 0L), WalMonitoring.empty(),
                        new MaintenanceStatsSnapshot(0L, 0L, 0L, 0L, 0L, 0L), 5L, 7L,
                        17L, 10, 5, 6, 7, SegmentIndexState.READY);

        final SegmentIndexRuntimeSnapshot snapshot = projection.project(collected);

        assertEquals(1L, snapshot.operations().readOperationCount());
        assertEquals(1L, snapshot.operations().putOperationCount());
        assertEquals(11L, snapshot.registryCache().hitCount());
        assertEquals(5, snapshot.chunkStoreCache().pageLimit());
        assertEquals(6L, snapshot.chunkStoreCache().hitCount());
        assertEquals(1, snapshot.segments().count());
        final SegmentIndexSegmentRuntimeMetrics segmentRuntime =
                snapshot.segments().runtimeMetrics().get(0);
        assertEquals(segmentId.getName(), segmentRuntime.segmentId());
        assertEquals(SegmentState.READY, segmentRuntime.state());
        assertEquals(5L, segmentRuntime.numberOfKeysInSegmentCache());
        assertEquals(2, segmentRuntime.numberOfKeysInWriteCache());
        assertEquals(3, segmentRuntime.numberOfDeltaCacheFiles());
        assertEquals(5L, segmentRuntime.compactRequestCount());
        assertEquals(7L, segmentRuntime.flushRequestCount());
        assertEquals(9L, segmentRuntime.bloomFilterRequestCount());
        assertEquals(1L, segmentRuntime.bloomFilterRefusedCount());
        assertEquals(8L, segmentRuntime.bloomFilterPositiveCount());
        assertEquals(1L, segmentRuntime.bloomFilterFalsePositiveCount());
        assertEquals(5L, snapshot.maintenance().compactRequestCount());
        assertEquals(7L, snapshot.maintenance().flushRequestCount());
        assertEquals(17L, snapshot.wal().appliedLsn());
        assertEquals(capturedAt, snapshot.capturedAt());
        assertFalse(snapshot.wal().enabled());
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("runtime-snapshot-projection"))
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
