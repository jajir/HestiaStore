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
            final int ioThreads) {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withName(indexName)//
                .withContextLoggingEnabled(false)//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInSegmentWriteCache(5)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInSegment(100)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withIndexWorkerThreadCount(1)//
                .withNumberOfIoThreads(ioThreads)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }
}
