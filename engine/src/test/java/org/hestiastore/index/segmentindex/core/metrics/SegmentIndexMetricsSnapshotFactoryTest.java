package org.hestiastore.index.segmentindex.core.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
import org.hestiastore.index.segmentindex.IndexWalConfiguration;
import org.hestiastore.index.segmentindex.core.control.RuntimeTuningState;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistry;
import org.hestiastore.index.segmentindex.core.executorregistry.ExecutorRegistryFixture;
import org.hestiastore.index.segmentindex.core.split.SplitMetricsSnapshot;
import org.hestiastore.index.segmentindex.wal.WalRuntime;
import org.hestiastore.index.segmentindex.wal.WalStats;
import org.hestiastore.index.segmentregistry.SegmentRegistryCacheStats;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
class SegmentIndexMetricsSnapshotFactoryTest {

    private WalRuntime<Integer, String> walRuntime;
    private ExecutorRegistry executorRegistry;

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
        executorRegistry = ExecutorRegistryFixture.from(conf);
        final SegmentIndexMetricsSnapshotFactory<Integer, String> factory =
                new SegmentIndexMetricsSnapshotFactory<>(conf,
                        () -> new SplitMetricsSnapshot(3, 2),
                        RuntimeTuningState.fromConfiguration(conf),
                        openDisabledWalRuntime(), stats, new AtomicLong(17L),
                        () -> SegmentIndexState.READY);
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
        walRuntime = WalRuntime.open(new MemDirectory(), IndexWalConfiguration.EMPTY, null, null);
        return walRuntime;
    }

    private static IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity.keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity.valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name("metrics-snapshot-factory"))
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
