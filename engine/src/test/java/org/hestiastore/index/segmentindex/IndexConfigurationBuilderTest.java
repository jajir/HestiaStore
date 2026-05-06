package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterSpecs;
import org.junit.jupiter.api.Test;

class IndexConfigurationBuilderTest {

    @Test
    void build_supportsIdentitySection() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .identity(identity -> identity.name("orders")
                        .keyClass(Integer.class).valueClass(String.class)
                        .keyTypeDescriptor("key-descriptor")
                        .valueTypeDescriptor("value-descriptor"))
                .build();

        assertEquals("orders", config.identity().name());
        assertEquals(Integer.class, config.identity().keyClass());
        assertEquals(String.class, config.identity().valueClass());
        assertEquals("key-descriptor", config.identity().keyTypeDescriptor());
        assertEquals("value-descriptor",
                config.identity().valueTypeDescriptor());
    }

    @Test
    void build_supportsGroupedSections() {
        final IndexConfiguration<Integer, String> grouped = IndexConfiguration
                .<Integer, String>builder()
                .identity(identity -> identity.name("grouped")
                        .keyClass(Integer.class).valueClass(String.class))
                .segment(segment -> segment.maxKeys(100).chunkKeyLimit(10)
                        .cacheKeyLimit(20).cachedSegmentLimit(4)
                        .deltaCacheFileLimit(3))
                .writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(7)
                        .maintenanceWriteCacheKeyLimit(9)
                        .indexBufferedWriteKeyLimit(40)
                        .segmentSplitKeyThreshold(80))
                .bloomFilter(bloom -> bloom.hashFunctions(2)
                        .indexSizeBytes(1024)
                        .falsePositiveProbability(0.05D))
                .maintenance(maintenance -> maintenance.segmentThreads(2)
                        .indexThreads(3).registryLifecycleThreads(4)
                        .busyBackoffMillis(5).busyTimeoutMillis(6)
                        .backgroundAutoEnabled(false))
                .io(io -> io.diskBufferSizeBytes(2048))
                .logging(logging -> logging.contextEnabled(false))
                .wal(wal -> wal.durability(WalDurabilityMode.SYNC)
                        .segmentSizeBytes(4096L))
                .filters(filters -> filters
                        .encodingFilterSpecs(
                                List.of(ChunkFilterSpecs.doNothing()))
                        .decodingFilterSpecs(
                                List.of(ChunkFilterSpecs.doNothing())))
                .build();

        assertEquals("grouped", grouped.identity().name());
        assertEquals(Integer.class, grouped.identity().keyClass());
        assertEquals(String.class, grouped.identity().valueClass());
        assertEquals(Integer.valueOf(100), grouped.segment().maxKeys());
        assertEquals(Integer.valueOf(10), grouped.segment().chunkKeyLimit());
        assertEquals(Integer.valueOf(20), grouped.segment().cacheKeyLimit());
        assertEquals(Integer.valueOf(3),
                grouped.segment().deltaCacheFileLimit());
        assertEquals(Integer.valueOf(7), grouped.writePath()
                .segmentWriteCacheKeyLimit());
        assertEquals(Integer.valueOf(9), grouped.writePath()
                .segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(Integer.valueOf(40),
                grouped.writePath().indexBufferedWriteKeyLimit());
        assertEquals(Integer.valueOf(80),
                grouped.writePath().segmentSplitKeyThreshold());
        assertEquals(Integer.valueOf(2),
                grouped.bloomFilter().hashFunctions());
        assertEquals(Integer.valueOf(1024),
                grouped.bloomFilter().indexSizeBytes());
        assertEquals(0.05D,
                grouped.bloomFilter().falsePositiveProbability());
        assertEquals(Integer.valueOf(2),
                grouped.maintenance().segmentThreads());
        assertEquals(Integer.valueOf(3), grouped.maintenance().indexThreads());
        assertEquals(Integer.valueOf(4),
                grouped.maintenance().registryLifecycleThreads());
        assertEquals(Integer.valueOf(5),
                grouped.maintenance().busyBackoffMillis());
        assertEquals(Integer.valueOf(6),
                grouped.maintenance().busyTimeoutMillis());
        assertFalse(grouped.maintenance().backgroundAutoEnabled());
        assertEquals(Integer.valueOf(2048),
                grouped.io().diskBufferSizeBytes());
        assertFalse(grouped.logging().contextEnabled());
        assertTrue(grouped.wal().isEnabled());
        assertEquals(List.of(ChunkFilterSpecs.doNothing()),
                grouped.filters().encodingChunkFilterSpecs());
        assertEquals(Integer.valueOf(4),
                grouped.runtimeTuning().maxSegmentsInCache());
    }

    @Test
    void build_groupedCallsUseLastWriter() {
        final IndexConfiguration<Integer, String> config =
                IndexConfiguration.<Integer, String>builder()
                        .segment(segment -> segment.cacheKeyLimit(10))
                        .segment(segment -> segment.cacheKeyLimit(20))
                        .build();

        assertEquals(20, config.segment().cacheKeyLimit());
    }

    @Test
    void build_derivesMaintenanceWriteCacheLimitWhenMissing() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(10))
                .build();

        assertEquals(14,
                config.writePath()
                        .segmentWriteCacheKeyLimitDuringMaintenance());
    }

    @Test
    void build_exposesCanonicalWritePathConfiguration() {
        final IndexConfiguration<Integer, String> config = IndexConfiguration
                .<Integer, String>builder()
                .writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(10)
                        .maintenanceWriteCacheKeyLimit(14)
                        .indexBufferedWriteKeyLimit(42)
                        .segmentSplitKeyThreshold(99))
                .build();

        assertEquals(10,
                config.writePath().segmentWriteCacheKeyLimit());
        assertEquals(14, config.writePath()
                .segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(42,
                config.writePath().indexBufferedWriteKeyLimit());
        assertEquals(99, config.writePath().segmentSplitKeyThreshold());
    }

    @Test
    void build_rejectsMaintenanceWriteCacheNotGreaterThanSegmentWriteCache() {
        final IndexConfigurationBuilder<Integer, String> builder = IndexConfiguration
                .<Integer, String>builder()
                .writePath(writePath -> writePath
                        .segmentWriteCacheKeyLimit(10)
                        .maintenanceWriteCacheKeyLimit(10));

        final IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, builder::build);

        assertEquals(
                "Property 'segmentWriteCacheKeyLimitDuringMaintenance' must be greater than 'segmentWriteCacheKeyLimit'",
                ex.getMessage());
    }
}
