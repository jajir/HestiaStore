package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.junit.jupiter.api.Test;

class HestiaStoreRuntimeTest {

    @Test
    void indexCloseDoesNotCloseCallerOwnedRuntime() {
        try (HestiaStoreRuntime runtime = HestiaStoreRuntime.builder()
                .segmentMaintenanceThreads(1)
                .splitMaintenanceThreads(1)
                .build()) {
            try (SegmentIndex<Integer, String> index = SegmentIndex.create(
                    new MemDirectory(), buildConf("shared-runtime-index"),
                    runtime)) {
                index.put(1, "one");
            }

            assertFalse(runtime.wasClosed());

            try (SegmentIndex<Integer, String> index = SegmentIndex.create(
                    new MemDirectory(),
                    buildConf("shared-runtime-reused-index"), runtime)) {
                index.put(2, "two");
            }
        }
    }

    @Test
    void closedRuntimeIsRejectedByIndexCreate() {
        final HestiaStoreRuntime runtime = HestiaStoreRuntime.builder()
                .segmentMaintenanceThreads(1)
                .splitMaintenanceThreads(1)
                .build();
        runtime.close();

        final IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> SegmentIndex.create(new MemDirectory(),
                        buildConf("closed-runtime-index"), runtime));

        assertEquals("HestiaStoreRuntime already closed", error.getMessage());
    }

    @Test
    void builderRejectsInvalidExecutorSettings() {
        final IllegalArgumentException segmentThreadsError = assertThrows(
                IllegalArgumentException.class,
                () -> HestiaStoreRuntime.builder()
                        .segmentMaintenanceThreads(0)
                        .build());
        assertEquals(
                "Property 'segmentMaintenanceThreads' must be greater than 0",
                segmentThreadsError.getMessage());

        final IllegalArgumentException splitThreadsError = assertThrows(
                IllegalArgumentException.class,
                () -> HestiaStoreRuntime.builder()
                        .splitMaintenanceThreads(0)
                        .build());
        assertEquals(
                "Property 'splitMaintenanceThreads' must be greater than 0",
                splitThreadsError.getMessage());

        final IllegalArgumentException prefixError = assertThrows(
                IllegalArgumentException.class,
                () -> HestiaStoreRuntime.builder()
                        .threadNamePrefix(" ")
                        .build());
        assertEquals("Property 'threadNamePrefix' must not be blank.",
                prefixError.getMessage());
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final String indexName) {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class))
                .identity(identity -> identity.valueClass(String.class))
                .identity(identity -> identity
                        .keyTypeDescriptor(new TypeDescriptorInteger()))
                .identity(identity -> identity
                        .valueTypeDescriptor(new TypeDescriptorShortString()))
                .identity(identity -> identity.name(indexName))
                .logging(logging -> logging.contextEnabled(false))
                .segment(segment -> segment.cacheKeyLimit(10))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))
                .writePath(writePath -> writePath
                        .maintenanceWriteCacheKeyLimit(6))
                .segment(segment -> segment.chunkKeyLimit(2))
                .segment(segment -> segment.maxKeys(100))
                .segment(segment -> segment.cachedSegmentLimit(3))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter
                        .falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .maintenance(maintenance -> maintenance.indexThreads(1))
                .maintenance(maintenance -> maintenance
                        .registryLifecycleThreads(1))
                .filters(filters -> filters
                        .encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters
                        .decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }
}
