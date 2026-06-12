package org.hestiastore.index.segmentindex.runtimemonitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;

import org.hestiastore.index.chunkstorecache.ChunkStoreCacheStats;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryStats;
import org.hestiastore.index.segmentindex.core.maintenance.MaintenanceStats;
import org.hestiastore.index.segmentindex.core.operations.IndexOperationStats;
import org.hestiastore.index.segmentindex.core.split.SplitStats;
import org.hestiastore.index.segmentindex.wal.WalMonitoring;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class CollectedRuntimeMonitoringDataTest {

    private ExecutorRegistry executorRegistry;

    @AfterEach
    void tearDown() {
        if (executorRegistry != null && !executorRegistry.wasClosed()) {
            executorRegistry.close();
        }
    }

    @Test
    void constructorKeepsValidatedScalarValues() {
        final CollectedRuntimeMonitoringData data = collected(1L, 2L, 3L, 4,
                5, 6, 7);

        assertEquals(1L, data.compactRequestCount());
        assertEquals(2L, data.flushRequestCount());
        assertEquals(3L, data.appliedWalLsn());
        assertEquals(4, data.segmentCacheKeyLimit());
        assertEquals(5, data.segmentWriteCacheKeyLimit());
        assertEquals(6, data.segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(7, data.indexBufferedWriteKeyLimit());
    }

    @Test
    void constructorRejectsNegativeScalarValues() {
        assertRejects("compactRequestCount",
                () -> collected(-1L, 0L, 0L, 0, 0, 0, 0));
        assertRejects("flushRequestCount",
                () -> collected(0L, -1L, 0L, 0, 0, 0, 0));
        assertRejects("appliedWalLsn",
                () -> collected(0L, 0L, -1L, 0, 0, 0, 0));
        assertRejects("segmentCacheKeyLimit",
                () -> collected(0L, 0L, 0L, -1, 0, 0, 0));
        assertRejects("segmentWriteCacheKeyLimit",
                () -> collected(0L, 0L, 0L, 0, -1, 0, 0));
        assertRejects("segmentWriteCacheKeyLimitDuringMaintenance",
                () -> collected(0L, 0L, 0L, 0, 0, -1, 0));
        assertRejects("indexBufferedWriteKeyLimit",
                () -> collected(0L, 0L, 0L, 0, 0, 0, -1));
    }

    private void assertRejects(final String propertyName,
            final Executable executable) {
        final IllegalArgumentException error =
                assertThrows(IllegalArgumentException.class, executable);
        assertEquals("Property '" + propertyName
                + "' must be greater than or equal to 0", error.getMessage());
    }

    @SuppressWarnings("java:S107")
    private CollectedRuntimeMonitoringData collected(
            final long compactRequestCount, final long flushRequestCount,
            final long appliedWalLsn, final int segmentCacheKeyLimit,
            final int segmentWriteCacheKeyLimit,
            final int segmentWriteCacheKeyLimitDuringMaintenance,
            final int indexBufferedWriteKeyLimit) {
        return new CollectedRuntimeMonitoringData(
                Instant.parse("2026-06-10T08:15:30Z"),
                new IndexOperationStats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
                new SegmentRegistryCacheStats(0L, 0L, 0L, 0L, 0, 0),
                new ChunkStoreCacheStats(0, 0, 0L, 0L, 0L, 0L, 0L, 0L),
                new StableSegmentRuntimeMetrics(),
                executorStats(),
                new SplitStats(0L, 0, 0, 0L, 0L),
                WalMonitoring.empty(),
                new MaintenanceStats(0L, 0L, 0L, 0L, 0L, 0L),
                compactRequestCount, flushRequestCount, appliedWalLsn,
                segmentCacheKeyLimit, segmentWriteCacheKeyLimit,
                segmentWriteCacheKeyLimitDuringMaintenance,
                indexBufferedWriteKeyLimit, SegmentIndexState.READY);
    }

    private ExecutorRegistryStats executorStats() {
        if (executorRegistry == null) {
            executorRegistry = ExecutorRegistryFixture.from(
                    SegmentIndexMetricsTestConfigurationFactory.build(
                            "collected-runtime-monitoring-data"));
        }
        return executorRegistry.statsSnapshot();
    }
}
