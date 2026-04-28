package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
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
