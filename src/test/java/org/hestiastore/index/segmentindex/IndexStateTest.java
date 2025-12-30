package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link SegmentIndexImpl} rejects operations after the index is
 * closed, exercising the {@link IndexState} lifecycle transitions.
 */
class IndexStateTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();

    /**
     * Ensures that all public operations fail with an
     * {@link IllegalStateException} once {@link SegmentIndexImpl#close()} has
     * completed.
     */
    @Test
    void operationsFailAfterClose() {
        final IndexInternalDefault<Integer, String> index = new IndexInternalDefault<>(
                org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                        .wrap(new MemDirectory()),
                tdi, tds, buildConf());
        index.put(1, "one");
        index.close();

        assertThrows(IllegalStateException.class, () -> index.put(2, "two"));
        assertThrows(IllegalStateException.class, () -> index.get(1));
        assertThrows(IllegalStateException.class, () -> index.delete(1));
        assertThrows(IllegalStateException.class, index::compact);
        assertThrows(IllegalStateException.class,
                index::checkAndRepairConsistency);
    }

    /**
     * Builds a minimal configuration for a small in-memory index used by the
     * lifecycle test.
     */
    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(tdi)//
                .withValueTypeDescriptor(tds)//
                .withName("index-state-test")//
                .withMaxNumberOfKeysInSegmentCache(4)//
                .withMaxNumberOfKeysInSegmentWriteCache(2)//
                .withMaxNumberOfKeysInSegmentCacheDuringFlushing(6)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withMaxNumberOfKeysInCache(10)//
                .withMaxNumberOfKeysInSegment(10)//
                .withMaxNumberOfSegmentsInCache(3)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(1024)//
                .withBloomFilterProbabilityOfFalsePositive(0.01D)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withContextLoggingEnabled(false)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }
}
