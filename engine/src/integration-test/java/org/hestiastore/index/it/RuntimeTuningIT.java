package org.hestiastore.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningPatch;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningResult;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningSnapshot;
import org.junit.jupiter.api.Test;

class RuntimeTuningIT {

    @Test
    void segmentPatchChangesCurrentSnapshotAndKeepsIndexUsable() {
        final Directory directory = new MemDirectory();
        try (SegmentIndex<Integer, String> index = SegmentIndex.create(
                directory, buildConf("runtime-tuning-segment-it"))) {
            final RuntimeTuningResult result = index.runtimeTuning()
                    .apply(RuntimeTuningPatch.builder()
                            .expectedRevision(index.runtimeTuning().current()
                                    .revision())
                            .segment(segment -> segment
                                    .cachedSegmentLimit(4)
                                    .cacheKeyLimit(30))
                            .build());

            assertTrue(result.applied());
            assertEquals(4, index.runtimeTuning().current().segment()
                    .cachedSegmentLimit());
            assertEquals(30, index.runtimeTuning().current().segment()
                    .cacheKeyLimit());
            final List<String> paths = changePaths(result);
            assertTrue(paths.contains("segment.cachedSegmentLimit"));
            assertTrue(paths.contains("segment.cacheKeyLimit"));
            index.put(1, "one");
            assertEquals("one", index.get(1));
        }
    }

    @Test
    void writePathPatchChangesCurrentSnapshotWithPathAwareChanges() {
        final Directory directory = new MemDirectory();
        try (SegmentIndex<Integer, String> index = SegmentIndex.create(
                directory, buildConf("runtime-tuning-write-path-it"))) {
            final RuntimeTuningResult result = index.runtimeTuning()
                    .apply(RuntimeTuningPatch.builder()
                            .expectedRevision(index.runtimeTuning().current()
                                    .revision())
                            .writePath(writePath -> writePath
                                    .segmentWriteCacheKeyLimit(6)
                                    .segmentWriteCacheKeyLimitDuringMaintenance(
                                            8)
                                    .indexBufferedWriteKeyLimit(12)
                                    .segmentSplitKeyThreshold(40))
                            .build());

            assertTrue(result.applied());
            final RuntimeTuningSnapshot current = index.runtimeTuning()
                    .current();
            assertEquals(6,
                    current.writePath().segmentWriteCacheKeyLimit());
            assertEquals(8, current.writePath()
                    .segmentWriteCacheKeyLimitDuringMaintenance());
            assertEquals(12,
                    current.writePath().indexBufferedWriteKeyLimit());
            assertEquals(40,
                    current.writePath().segmentSplitKeyThreshold());
            final List<String> paths = changePaths(result);
            assertTrue(paths
                    .contains("writePath.segmentWriteCacheKeyLimit"));
            assertTrue(paths.contains(
                    "writePath.segmentWriteCacheKeyLimitDuringMaintenance"));
            assertTrue(paths
                    .contains("writePath.indexBufferedWriteKeyLimit"));
            assertTrue(paths
                    .contains("writePath.segmentSplitKeyThreshold"));
        }
    }

    @Test
    void chunkStoreCachePatchChangesLimitAndMetrics() {
        final Directory directory = new MemDirectory();
        try (SegmentIndex<Integer, String> index = SegmentIndex.create(
                directory, buildConf("runtime-tuning-chunk-cache-it"))) {
            final RuntimeTuningResult result = index.runtimeTuning()
                    .apply(RuntimeTuningPatch.builder()
                            .expectedRevision(index.runtimeTuning().current()
                                    .revision())
                            .chunkStoreCache(cache -> cache.pageLimit(5))
                            .build());

            assertTrue(result.applied());
            assertEquals(5, index.runtimeTuning().current()
                    .chunkStoreCache().pageLimit());
            assertEquals(5, index.runtimeMonitoring().snapshot().getMetrics()
                    .getChunkStoreCachePageLimit());
            assertTrue(changePaths(result)
                    .contains("chunkStoreCache.pageLimit"));
        }
    }

    @Test
    void runtimeTuningIsDurableOnlyAfterPersistCurrent() {
        final Directory directory = new MemDirectory();
        try (SegmentIndex<Integer, String> index = SegmentIndex.create(
                directory, buildConf("runtime-tuning-durability-it"))) {
            applyFullPatch(index);
            assertSectionValues(index.runtimeTuning().current(), 4, 30, 6, 8,
                    12, 40);
        }

        try (SegmentIndex<Integer, String> reopened = SegmentIndex
                .open(directory)) {
            assertSectionValues(reopened.runtimeTuning().current(), 3, 10, 5,
                    7, 9, 50);
            assertSectionValues(reopened.runtimeTuning().original(), 3, 10, 5,
                    7, 9, 50);
            applyFullPatch(reopened);
            assertSectionValues(reopened.runtimeTuning().persistCurrent(), 4,
                    30, 6, 8, 12, 40);
        }

        try (SegmentIndex<Integer, String> reopened = SegmentIndex
                .open(directory)) {
            assertSectionValues(reopened.runtimeTuning().current(), 4, 30, 6,
                    8, 12, 40);
            assertSectionValues(reopened.runtimeTuning().original(), 4, 30, 6,
                    8, 12, 40);
        }
    }

    private static void applyFullPatch(
            final SegmentIndex<Integer, String> index) {
        final RuntimeTuningResult result = index.runtimeTuning()
                .apply(RuntimeTuningPatch.builder()
                        .expectedRevision(index.runtimeTuning().current()
                                .revision())
                        .segment(segment -> segment
                                .cachedSegmentLimit(4)
                                .cacheKeyLimit(30))
                        .writePath(writePath -> writePath
                                .segmentWriteCacheKeyLimit(6)
                                .segmentWriteCacheKeyLimitDuringMaintenance(8)
                                .indexBufferedWriteKeyLimit(12)
                                .segmentSplitKeyThreshold(40))
                        .chunkStoreCache(cache -> cache.pageLimit(2))
                        .build());
        assertTrue(result.applied());
    }

    private static void assertSectionValues(final RuntimeTuningSnapshot snapshot,
            final int cachedSegmentLimit, final int cacheKeyLimit,
            final int segmentWriteCacheKeyLimit,
            final int segmentWriteCacheKeyLimitDuringMaintenance,
            final int indexBufferedWriteKeyLimit,
            final int segmentSplitKeyThreshold) {
        assertEquals(cachedSegmentLimit,
                snapshot.segment().cachedSegmentLimit());
        assertEquals(cacheKeyLimit, snapshot.segment().cacheKeyLimit());
        assertEquals(segmentWriteCacheKeyLimit,
                snapshot.writePath().segmentWriteCacheKeyLimit());
        assertEquals(segmentWriteCacheKeyLimitDuringMaintenance,
                snapshot.writePath()
                        .segmentWriteCacheKeyLimitDuringMaintenance());
        assertEquals(indexBufferedWriteKeyLimit,
                snapshot.writePath().indexBufferedWriteKeyLimit());
        assertEquals(segmentSplitKeyThreshold,
                snapshot.writePath().segmentSplitKeyThreshold());
    }

    private static List<String> changePaths(final RuntimeTuningResult result) {
        return result.changes().stream().map(change -> change.field().path())
                .toList();
    }

    private static IndexConfiguration<Integer, String> buildConf(
            final String indexName) {
        return IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class)
                        .valueClass(String.class)
                        .keyTypeDescriptor(new TypeDescriptorInteger())
                        .valueTypeDescriptor(new TypeDescriptorShortString())
                        .name(indexName))
                .segment(segment -> segment.cacheKeyLimit(10)
                        .cachedSegmentLimit(3)
                        .chunkKeyLimit(2)
                        .maxKeys(100))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5)
                        .maintenanceWriteCacheKeyLimit(7)
                        .indexBufferedWriteKeyLimit(9)
                        .segmentSplitKeyThreshold(50))
                .logging(logging -> logging.contextEnabled(false))
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024)
                        .hashFunctions(1)
                        .falsePositiveProbability(0.01D))
                .io(io -> io.diskBufferSizeBytes(1024))
                .filters(filters -> filters
                        .encodingFilters(List.of(new ChunkFilterDoNothing())))
                .filters(filters -> filters
                        .decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }
}
