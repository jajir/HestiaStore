package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;

class SegmentIndexImplTest {

    @Test
    void putGetAndDeleteRoundTrip() {
        final IndexInternalSynchronized<Integer, String> index = new IndexInternalSynchronized<>(
                AsyncDirectoryAdapter.wrap(new MemDirectory()),
                new TypeDescriptorInteger(),
                new TypeDescriptorShortString(),
                buildConf());
        try {
            index.put(1, "one");

            assertEquals("one", index.get(1));

            index.delete(1);
            assertNull(index.get(1));
        } finally {
            index.close();
        }
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("segment-index-impl-test")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInSegmentWriteCache(5)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringFlush(6)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInCache(10)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withNumberOfCpuThreads(1)
                .withNumberOfIoThreads(1)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }
}
