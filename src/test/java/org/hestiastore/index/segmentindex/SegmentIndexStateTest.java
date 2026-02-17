package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegmentIndexStateTest {

    private SegmentIndex<Integer, String> index;
    private IndexInternalConcurrent<Integer, String> errorIndex;

    @BeforeEach
    void setUp() {
        index = SegmentIndex.create(new MemDirectory(), buildConf());
        errorIndex = new IndexInternalConcurrent<>(
                new MemDirectory(),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                buildConf());
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
        if (errorIndex != null && !errorIndex.wasClosed()) {
            errorIndex.close();
        }
    }

    @Test
    void readyAndClosedStatesAreExposed() {
        assertEquals(SegmentIndexState.READY, index.getState());
        index.close();
        assertEquals(SegmentIndexState.CLOSED, index.getState());
    }

    @Test
    void errorStateRejectsOperations() {
        errorIndex.failWithError(new IllegalStateException("boom"));
        assertEquals(SegmentIndexState.ERROR, errorIndex.getState());
        assertThrows(IllegalStateException.class, () -> errorIndex.get(1));
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("segment-index-state-test")
                .withContextLoggingEnabled(false)
                .withMaxNumberOfKeysInSegmentCache(10)
                .withMaxNumberOfKeysInSegmentWriteCache(5)
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(6)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withMaxNumberOfKeysInSegment(100)
                .withMaxNumberOfSegmentsInCache(3)
                .withBloomFilterNumberOfHashFunctions(1)
                .withBloomFilterIndexSizeInBytes(1024)
                .withBloomFilterProbabilityOfFalsePositive(0.01D)
                .withDiskIoBufferSizeInBytes(1024)
                .withIndexWorkerThreadCount(1)
                .withNumberOfIoThreads(1)
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))
                .build();
    }
}
