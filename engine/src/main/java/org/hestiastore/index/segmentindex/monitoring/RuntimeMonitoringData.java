package org.hestiastore.index.segmentindex.monitoring;

import java.time.Instant;

import org.hestiastore.index.Vldtn;
import org.hestiastore.index.chunkstorecache.ChunkStoreCacheStats;
import org.hestiastore.index.segmentindex.SegmentIndexState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryStats;
import org.hestiastore.index.segmentindex.core.execution.MaintenanceStatsSnapshot;
import org.hestiastore.index.segmentindex.core.execution.OperationStatsSnapshot;
import org.hestiastore.index.segmentindex.core.split.SplitStats;
import org.hestiastore.index.segmentindex.wal.WalMonitoring;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;

/**
 * Internal aggregate of package-owned monitoring data collected at one point in
 * time.
 */
@SuppressWarnings("java:S107")
final class RuntimeMonitoringData {

    private final Instant capturedAt;
    private final OperationStatsSnapshot operationStats;
    private final SegmentRegistryCacheStats registryCacheStats;
    private final ChunkStoreCacheStats chunkStoreCacheStats;
    private final SegmentRuntimeMetrics stableSegmentRuntime;
    private final ExecutorRegistryStats executorStats;
    private final SplitStats splitStats;
    private final WalMonitoring walMonitoring;
    private final MaintenanceStatsSnapshot maintenanceStats;
    private final long compactRequestCount;
    private final long flushRequestCount;
    private final long appliedWalLsn;
    private final int segmentCacheKeyLimit;
    private final int segmentWriteCacheKeyLimit;
    private final int segmentWriteCacheKeyLimitDuringMaintenance;
    private final int indexBufferedWriteKeyLimit;
    private final SegmentIndexState state;

    RuntimeMonitoringData(final Instant capturedAt,
            final OperationStatsSnapshot operationStats,
            final SegmentRegistryCacheStats registryCacheStats,
            final ChunkStoreCacheStats chunkStoreCacheStats,
            final SegmentRuntimeMetrics stableSegmentRuntime,
            final ExecutorRegistryStats executorStats,
            final SplitStats splitStats, final WalMonitoring walMonitoring,
            final MaintenanceStatsSnapshot maintenanceStats,
            final long compactRequestCount, final long flushRequestCount,
            final long appliedWalLsn, final int segmentCacheKeyLimit,
            final int segmentWriteCacheKeyLimit,
            final int segmentWriteCacheKeyLimitDuringMaintenance,
            final int indexBufferedWriteKeyLimit,
            final SegmentIndexState state) {
        this.capturedAt = Vldtn.requireNonNull(capturedAt, "capturedAt");
        this.operationStats = Vldtn.requireNonNull(operationStats,
                "operationStats");
        this.registryCacheStats = Vldtn.requireNonNull(registryCacheStats,
                "registryCacheStats");
        this.chunkStoreCacheStats = Vldtn.requireNonNull(chunkStoreCacheStats,
                "chunkStoreCacheStats");
        this.stableSegmentRuntime = Vldtn.requireNonNull(stableSegmentRuntime,
                "stableSegmentRuntime");
        this.executorStats = Vldtn.requireNonNull(executorStats,
                "executorStats");
        this.splitStats = Vldtn.requireNonNull(splitStats, "splitStats");
        this.walMonitoring = Vldtn.requireNonNull(walMonitoring, "walMonitoring");
        this.maintenanceStats = Vldtn.requireNonNull(maintenanceStats,
                "maintenanceStats");
        this.compactRequestCount = Vldtn.requireGreaterThanOrEqualToZero(
                compactRequestCount, "compactRequestCount");
        this.flushRequestCount = Vldtn.requireGreaterThanOrEqualToZero(
                flushRequestCount, "flushRequestCount");
        this.appliedWalLsn = Vldtn.requireGreaterThanOrEqualToZero(
                appliedWalLsn, "appliedWalLsn");
        this.segmentCacheKeyLimit = Vldtn.requireGreaterThanOrEqualToZero(
                segmentCacheKeyLimit, "segmentCacheKeyLimit");
        this.segmentWriteCacheKeyLimit = Vldtn.requireGreaterThanOrEqualToZero(
                segmentWriteCacheKeyLimit, "segmentWriteCacheKeyLimit");
        this.segmentWriteCacheKeyLimitDuringMaintenance =
                Vldtn.requireGreaterThanOrEqualToZero(
                        segmentWriteCacheKeyLimitDuringMaintenance,
                        "segmentWriteCacheKeyLimitDuringMaintenance");
        this.indexBufferedWriteKeyLimit =
                Vldtn.requireGreaterThanOrEqualToZero(
                        indexBufferedWriteKeyLimit,
                        "indexBufferedWriteKeyLimit");
        this.state = Vldtn.requireNonNull(state, "state");
    }

    Instant capturedAt() {
        return capturedAt;
    }

    OperationStatsSnapshot operationStats() {
        return operationStats;
    }

    SegmentRegistryCacheStats registryCacheStats() {
        return registryCacheStats;
    }

    ChunkStoreCacheStats chunkStoreCacheStats() {
        return chunkStoreCacheStats;
    }

    SegmentRuntimeMetrics stableSegmentRuntime() {
        return stableSegmentRuntime;
    }

    ExecutorRegistryStats executorStats() {
        return executorStats;
    }

    SplitStats splitStats() {
        return splitStats;
    }

    WalMonitoring walMonitoring() {
        return walMonitoring;
    }

    MaintenanceStatsSnapshot maintenanceStats() {
        return maintenanceStats;
    }

    long compactRequestCount() {
        return compactRequestCount;
    }

    long flushRequestCount() {
        return flushRequestCount;
    }

    long appliedWalLsn() {
        return appliedWalLsn;
    }

    int segmentCacheKeyLimit() {
        return segmentCacheKeyLimit;
    }

    int segmentWriteCacheKeyLimit() {
        return segmentWriteCacheKeyLimit;
    }

    int segmentWriteCacheKeyLimitDuringMaintenance() {
        return segmentWriteCacheKeyLimitDuringMaintenance;
    }

    int indexBufferedWriteKeyLimit() {
        return indexBufferedWriteKeyLimit;
    }

    SegmentIndexState state() {
        return state;
    }
}
