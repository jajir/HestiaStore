package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.segmentindex.configuration.api.IndexConfiguration;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningResult;
import org.hestiastore.index.segmentindex.configuration.tuning.RuntimeTuningPatch;
import org.junit.jupiter.api.Test;

class SegmentIndexTest {

    @Test
    void createAndTryOpen() {
        final MemDirectory directory = new MemDirectory();

        final Optional<SegmentIndex<Integer, String>> beforeCreate = SegmentIndex
                .tryOpen(directory);
        assertTrue(beforeCreate.isEmpty());

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                buildConf("segment-index-test", 1))) {
            index.put(1, "one");
            assertEquals("one", index.get(1));
        }

        final Optional<SegmentIndex<Integer, String>> reopened = SegmentIndex
                .tryOpen(directory);
        assertTrue(reopened.isPresent());
        reopened.get().close();
    }

    @Test
    void openWithStoredConfiguration() {
        final MemDirectory directory = new MemDirectory();
        try (SegmentIndex<Integer, String> created = SegmentIndex.create(
                directory, buildConf("segment-index-open-stored", 1))) {
            created.put(1, "one");
        }

        try (SegmentIndex<Integer, String> opened = SegmentIndex.open(directory)) {
            assertEquals("one", opened.get(1));
        }
    }

    @Test
    void openWithOverrideConfiguration() {
        final MemDirectory directory = new MemDirectory();
        final String indexName = "segment-index-open-override";
        try (SegmentIndex<Integer, String> created = SegmentIndex.create(
                directory, buildConf(indexName, 1))) {
            created.put(1, "one");
        }

        try (SegmentIndex<Integer, String> opened = SegmentIndex.open(directory,
                buildConf(indexName, 2))) {
            assertEquals("one", opened.get(1));
            opened.put(2, "two");
            assertEquals("two", opened.get(2));
        }
    }

    @Test
    void openNonExistingIndexThrowsException() {
        final MemDirectory directory = new MemDirectory();

        assertThrows(IndexException.class,
                () -> SegmentIndex.open(directory,
                        buildConf("segment-index-open-missing", 1)));
    }

    @Test
    void createAlreadyExistingIndexThrowsException() {
        final MemDirectory directory = new MemDirectory();
        try (SegmentIndex<Integer, String> ignored = SegmentIndex.create(
                directory, buildConf("segment-index-create-existing", 1))) {
            ignored.put(1, "one");
            assertEquals("one", ignored.get(1));
        }

        assertThrows(IndexException.class,
                () -> SegmentIndex.create(directory,
                        buildConf("segment-index-create-existing-new", 1)));
    }

    @Test
    void runtimeTuningIsDurableOnlyAfterPersistCurrent() {
        final MemDirectory directory = new MemDirectory();
        try (SegmentIndex<Integer, String> created = SegmentIndex.create(
                directory, buildConf("runtime-tuning-durable", 1))) {
            applySegmentCacheKeyLimit(created, 30);
            assertEquals(Integer.valueOf(30), created.runtimeTuning()
                    .current().segment().cacheKeyLimit());
        }

        try (SegmentIndex<Integer, String> opened = SegmentIndex
                .open(directory)) {
            assertEquals(Integer.valueOf(10), opened.runtimeTuning()
                    .current().segment().cacheKeyLimit());
            applySegmentCacheKeyLimit(opened, 40);
            assertEquals(Integer.valueOf(40), opened.runtimeTuning()
                    .persistCurrent().segment().cacheKeyLimit());
        }

        try (SegmentIndex<Integer, String> opened = SegmentIndex
                .open(directory)) {
            assertEquals(Integer.valueOf(40), opened.runtimeTuning()
                    .current().segment().cacheKeyLimit());
            assertEquals(Integer.valueOf(40), opened.runtimeTuning()
                    .original().segment().cacheKeyLimit());
            assertEquals(0L, opened.runtimeTuning().current().revision());
        }
    }

    private void applySegmentCacheKeyLimit(
            final SegmentIndex<Integer, String> index, final int value) {
        final long revision = index.runtimeTuning().current().revision();
        final RuntimeTuningResult patchResult = index.runtimeTuning()
                .apply(RuntimeTuningPatch.builder()
                        .expectedRevision(revision)
                        .cacheKeyLimit(value)
                        .build());
        assertTrue(patchResult.applied());
    }

    private IndexConfiguration<Integer, String> buildConf(final String indexName,
            final int indexWorkerThreads) {
        return IndexConfiguration.<Integer, String>builder()//
                .identity(identity -> identity.keyClass(Integer.class))//
                .identity(identity -> identity.valueClass(String.class))//
                .identity(identity -> identity
                        .keyTypeDescriptor(new TypeDescriptorInteger()))//
                .identity(identity -> identity
                        .valueTypeDescriptor(new TypeDescriptorShortString()))//
                .identity(identity -> identity.name(indexName))//
                .logging(logging -> logging.contextEnabled(false))//
                .segment(segment -> segment.cacheKeyLimit(10))//
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(5))//
                .writePath(writePath -> writePath.maintenanceWriteCacheKeyLimit(6))//
                .segment(segment -> segment.chunkKeyLimit(2))//
                .segment(segment -> segment.maxKeys(100))//
                .segment(segment -> segment.cachedSegmentLimit(3))//
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1))//
                .bloomFilter(bloomFilter -> bloomFilter.indexSizeBytes(1024))//
                .bloomFilter(bloomFilter -> bloomFilter.falsePositiveProbability(0.01D))//
                .io(io -> io.diskBufferSizeBytes(1024))//
                .filters(filters -> filters
                        .encodingFilters(List.of(new ChunkFilterDoNothing())))//
                .filters(filters -> filters
                        .decodingFilters(List.of(new ChunkFilterDoNothing())))//
                .build();
    }
}
