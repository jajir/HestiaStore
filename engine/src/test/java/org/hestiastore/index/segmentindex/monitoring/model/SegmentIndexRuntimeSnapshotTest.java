package org.hestiastore.index.segmentindex.monitoring.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.List;

import org.hestiastore.index.segment.SegmentState;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.junit.jupiter.api.Test;

class SegmentIndexRuntimeSnapshotTest {

    @Test
    void constructorStoresGroupedMetricSections() {
        final SegmentIndexOperationMetrics operations =
                new SegmentIndexOperationMetrics(1L, 2L, 3L);
        final SegmentIndexRegistryCacheMetrics registryCache =
                new SegmentIndexRegistryCacheMetrics(4L, 5L, 6L, 7L, 8, 9);
        final SegmentIndexChunkStoreCacheMetrics chunkStoreCache =
                new SegmentIndexChunkStoreCacheMetrics(10, 11, 12L, 13L, 14L,
                        15L, 16L, 17L);
        final SegmentIndexSegmentRuntimeMetrics runtimeMetrics =
                new SegmentIndexSegmentRuntimeMetrics("segment-1",
                        SegmentState.READY, 18L, 19L, 20L, 21L, 22, 23, 24L,
                        25L, 26L, 27L, 28L, 29L);
        final SegmentIndexSegmentMetrics segments =
                new SegmentIndexSegmentMetrics(30, 31, 32, 33, 34, 35, 36,
                        37L, 38L, 39L, List.of(runtimeMetrics));
        final SegmentIndexWritePathMetrics writePath =
                new SegmentIndexWritePathMetrics(40, 41, 42, 43L);
        final SegmentIndexExecutorMetrics indexExecutor =
                new SegmentIndexExecutorMetrics(47, 48, 49, 50L, 51L, 52L);
        final SegmentIndexExecutorMetrics stableExecutor =
                new SegmentIndexExecutorMetrics(53, 54, 55, 56L, 57L, 58L);
        final SegmentIndexMaintenanceMetrics maintenance =
                new SegmentIndexMaintenanceMetrics(59L, 60L, 61L, 62L, 63L,
                        64L, indexExecutor, stableExecutor);
        final SegmentIndexExecutorMetrics splitExecutor =
                new SegmentIndexExecutorMetrics(65, 66, 67, 68L, 69L, 70L);
        final SegmentIndexSplitMetrics split =
                new SegmentIndexSplitMetrics(71L, 72, 73, 74L, 75L,
                        splitExecutor);
        final SegmentIndexLatencyMetrics latency =
                new SegmentIndexLatencyMetrics(76L, 77L, 78L, 79L, 80L, 81L);
        final SegmentIndexBloomFilterMetrics bloomFilter =
                new SegmentIndexBloomFilterMetrics(3, 1024, 0.01D, 82L, 83L,
                        84L, 85L);
        final SegmentIndexWalMetrics wal =
                new SegmentIndexWalMetrics(true, 86L, 87L, 88L, 89L, 90L,
                        91L, 92L, 93, 94L, 95L, 96L, 97L, 98L, 99L, 100L,
                        101L);
        final Instant capturedAt = Instant.parse("2026-06-09T00:00:00Z");

        final SegmentIndexRuntimeSnapshot snapshot =
                new SegmentIndexRuntimeSnapshot("orders", SegmentIndexState.READY,
                        capturedAt, operations, registryCache,
                        chunkStoreCache, segments, writePath, maintenance,
                        split, latency, bloomFilter, wal);

        assertEquals("orders", snapshot.indexName());
        assertSame(operations, snapshot.operations());
        assertSame(registryCache, snapshot.registryCache());
        assertSame(chunkStoreCache, snapshot.chunkStoreCache());
        assertSame(segments, snapshot.segments());
        assertSame(writePath, snapshot.writePath());
        assertSame(maintenance, snapshot.maintenance());
        assertSame(split, snapshot.split());
        assertSame(latency, snapshot.latency());
        assertSame(bloomFilter, snapshot.bloomFilter());
        assertSame(wal, snapshot.wal());
        assertEquals(SegmentIndexState.READY, snapshot.state());
        assertEquals(capturedAt, snapshot.capturedAt());
        assertEquals(1L, snapshot.operations().readOperationCount());
        assertEquals(11, snapshot.chunkStoreCache().pageCount());
        assertEquals("segment-1",
                snapshot.segments().runtimeMetrics().get(0).segmentId());
        assertEquals(52L,
                snapshot.maintenance().indexExecutor().callerRunsCount());
        assertEquals(75L, snapshot.split().taskRunLatencyP95Micros());
        assertEquals(2L, snapshot.wal().checkpointLagLsn());
    }

    @Test
    void constructorRejectsNullMetricSection() {
        final SegmentIndexRegistryCacheMetrics registryCache =
                new SegmentIndexRegistryCacheMetrics(0L, 0L, 0L, 0L, 0, 0);
        final SegmentIndexChunkStoreCacheMetrics chunkStoreCache =
                new SegmentIndexChunkStoreCacheMetrics(0, 0, 0L, 0L, 0L, 0L,
                        0L, 0L);
        final SegmentIndexSegmentMetrics segments =
                new SegmentIndexSegmentMetrics(0, 0, 0, 0, 0, 0, 0, 0L, 0L,
                        0L, List.of());
        final SegmentIndexWritePathMetrics writePath =
                new SegmentIndexWritePathMetrics(0, 0, 0, 0L);
        final SegmentIndexExecutorMetrics executor =
                new SegmentIndexExecutorMetrics(0, 0, 0, 0L, 0L, 0L);
        final SegmentIndexMaintenanceMetrics maintenance =
                new SegmentIndexMaintenanceMetrics(0L, 0L, 0L, 0L, 0L, 0L,
                        executor, executor);
        final SegmentIndexSplitMetrics split =
                new SegmentIndexSplitMetrics(0L, 0, 0, 0L, 0L, executor);
        final SegmentIndexLatencyMetrics latency =
                new SegmentIndexLatencyMetrics(0L, 0L, 0L, 0L, 0L, 0L);
        final SegmentIndexBloomFilterMetrics bloomFilter =
                new SegmentIndexBloomFilterMetrics(0, 0, 0D, 0L, 0L, 0L, 0L);
        final SegmentIndexWalMetrics wal =
                new SegmentIndexWalMetrics(false, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                        0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new SegmentIndexRuntimeSnapshot("orders", SegmentIndexState.READY,
                        Instant.EPOCH, null, registryCache, chunkStoreCache,
                        segments, writePath, maintenance, split, latency,
                        bloomFilter, wal));

        assertEquals("Property 'operations' must not be null.",
                ex.getMessage());
    }
}
