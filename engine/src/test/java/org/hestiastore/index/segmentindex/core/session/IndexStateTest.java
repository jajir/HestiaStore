package org.hestiastore.index.segmentindex.core.session;

import org.hestiastore.index.segmentindex.core.maintenance.IndexExecutorRegistry;
import org.hestiastore.index.segmentindex.core.session.IndexInternalConcurrent;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.core.session.SegmentIndexImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link SegmentIndexImpl} rejects operations after the index is
 * closed, exercising the {@link IndexState} lifecycle transitions.
 */
class IndexStateTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();
    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    private IndexInternalConcurrent<Integer, String> index;

    @BeforeEach
    void setUp() {
        final IndexConfiguration<Integer, String> conf = buildConf();
        index = new IndexInternalConcurrent<>(
                new MemDirectory(),
                tdi, tds, conf, conf.resolveRuntimeConfiguration(),
                new IndexExecutorRegistry(conf));
    }

    @AfterEach
    void tearDown() {
        if (index != null && !index.wasClosed()) {
            index.close();
        }
    }

    /**
     * Ensures that all public operations fail with an
     * {@link IllegalStateException} once {@link SegmentIndexImpl#close()} has
     * completed.
     */
    @Test
    void operationsFailAfterClose() {
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
                .withMaxNumberOfKeysInActivePartition(2)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
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
