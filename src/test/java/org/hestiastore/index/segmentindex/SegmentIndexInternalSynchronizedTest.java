package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SegmentIndexInternalSynchronizedTest {

    private static final TypeDescriptor<Integer> TD_INTEGER = new TypeDescriptorInteger();
    private static final TypeDescriptor<String> TD_STRING = new TypeDescriptorShortString();

    private Directory directory = new MemDirectory();

    @Test
    void test_constructor() {
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(TD_INTEGER)//
                .withValueTypeDescriptor(TD_STRING)//
                .withName("test-index")//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(10)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInCache(10)//
                .withMaxNumberOfKeysInReadCache(0)//
                .withMaxNumberOfKeysInSegment(10)//
                .withMaxNumberOfSegmentsInCache(10)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withContextLoggingEnabled(false)//
                .withThreadSafe(true)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
        try (IndexInternalSynchronized<Integer, String> synchIndex = new IndexInternalSynchronized<>(
                directory, TD_INTEGER, TD_STRING, conf)) {
            // Intentionally left empty to test constructor
        }

        // If no exception is thrown, then constructor works as expected
        assertTrue(true);
    }

}
