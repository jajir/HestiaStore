package org.hestiastore.index.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.chunkstore.ChunkFilterCrc32Validation;
import org.hestiastore.index.chunkstore.ChunkFilterCrc32Writing;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberValidation;
import org.hestiastore.index.chunkstore.ChunkFilterMagicNumberWriting;
import org.hestiastore.index.chunkstore.ChunkFilterSnappyCompress;
import org.hestiastore.index.chunkstore.ChunkFilterSnappyDecompress;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentIndex;
import org.hestiastore.index.segmentindex.monitoring.model.SegmentIndexRuntimeSnapshot;
import org.junit.jupiter.api.Test;

class ParsedChunkPageCacheIT {

    @Test
    void persistedReadReturnsValueThroughParsedPageCache() {
        final Directory directory = new MemDirectory();
        seed(directory, "parsed-page-cache-read", false);

        try (SegmentIndex<Integer, String> index = SegmentIndex.open(
                directory)) {
            assertEquals("value-3", index.get(3));
            assertEquals("value-3", index.get(3));

            final SegmentIndexRuntimeSnapshot metrics = metrics(index);
            assertEquals(4, metrics.chunkStoreCache().pageLimit());
            assertEquals(1, metrics.chunkStoreCache().pageCount());
            assertEquals(1L, metrics.chunkStoreCache().missCount());
            assertEquals(1L, metrics.chunkStoreCache().loadCount());
            assertEquals(1L, metrics.chunkStoreCache().hitCount());
        }
    }

    @Test
    void putAndDeleteShadowCachedPersistedPage() {
        final Directory directory = new MemDirectory();
        seed(directory, "parsed-page-cache-overlay", false);

        try (SegmentIndex<Integer, String> index = SegmentIndex.open(
                directory)) {
            assertEquals("value-2", index.get(2));

            index.put(2, "overlay-2");
            assertEquals("overlay-2", index.get(2));

            index.delete(2);
            assertNull(index.get(2));
            assertEquals(1L, metrics(index).chunkStoreCache().loadCount());
        }
    }

    @Test
    void compactionInvalidatesCachedOwnerPagesBeforeVersionSwitchRead() {
        final Directory directory = new MemDirectory();
        seed(directory, "parsed-page-cache-compaction", false);

        try (SegmentIndex<Integer, String> index = SegmentIndex.open(
                directory)) {
            assertEquals("value-1", index.get(1));
            assertEquals(1, metrics(index).chunkStoreCache().pageCount());

            index.put(1, "new-1");
            index.maintenance().compactAndWait();

            final SegmentIndexRuntimeSnapshot afterCompaction = metrics(index);
            assertEquals(0, afterCompaction.chunkStoreCache().pageCount());
            assertTrue(afterCompaction.chunkStoreCache().invalidationCount()
                    > 0L);
            assertEquals("new-1", index.get(1));
        }
    }

    @Test
    void snappyFilteredIndexReadsThroughParsedPageCache() {
        final Directory directory = new MemDirectory();
        seed(directory, "parsed-page-cache-snappy", true);

        try (SegmentIndex<Integer, String> index = SegmentIndex.open(
                directory)) {
            assertEquals("value-4", index.get(4));
            assertEquals("value-4", index.get(4));

            final SegmentIndexRuntimeSnapshot metrics = metrics(index);
            assertEquals(1L, metrics.chunkStoreCache().loadCount());
            assertEquals(1L, metrics.chunkStoreCache().hitCount());
        }
    }

    private static void seed(final Directory directory, final String indexName,
            final boolean snappy) {
        try (SegmentIndex<Integer, String> index = SegmentIndex.create(
                directory, configuration(indexName, snappy))) {
            for (int key = 0; key < 8; key++) {
                index.put(key, "value-" + key);
            }
            index.maintenance().compactAndWait();
        }
    }

    private static SegmentIndexRuntimeSnapshot metrics(
            final SegmentIndex<Integer, String> index) {
        return index.runtimeMonitoring().snapshot();
    }

    private static IndexConfiguration<Integer, String> configuration(
            final String indexName, final boolean snappy) {
        final var builder = IndexConfiguration.<Integer, String>builder()
                .identity(identity -> identity.keyClass(Integer.class)
                        .valueClass(String.class).name(indexName))
                .segment(segment -> segment.cacheKeyLimit(8)
                        .cachedSegmentLimit(3).chunkKeyLimit(2).maxKeys(128)
                        .deltaCacheFileLimit(2))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(16)
                        .maintenanceWriteCacheKeyLimit(32)
                        .indexBufferedWriteKeyLimit(64)
                        .segmentSplitKeyThreshold(128))
                .bloomFilter(bloom -> bloom.indexSizeBytes(1024)
                        .hashFunctions(3).falsePositiveProbability(0.01D))
                .maintenance(maintenance -> maintenance
                        .backgroundAutoEnabled(false))
                .chunkStoreCache(cache -> cache.pageLimit(4))
                .logging(logging -> logging.contextEnabled(false));
        if (snappy) {
            builder.filters(filters -> filters
                    .addEncodingFilter(new ChunkFilterCrc32Writing())
                    .addEncodingFilter(new ChunkFilterMagicNumberWriting())
                    .addEncodingFilter(new ChunkFilterSnappyCompress())
                    .addDecodingFilter(new ChunkFilterMagicNumberValidation())
                    .addDecodingFilter(new ChunkFilterSnappyDecompress())
                    .addDecodingFilter(new ChunkFilterCrc32Validation()));
        }
        return builder.build();
    }
}
