package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class IntegrationSegmentIndexMetricsSnapshotTest {

    @Test
    void metricsSnapshotCountsPointOperations() {
        final Directory directory = new MemDirectory();
        final TypeDescriptorInteger keyDescriptor = new TypeDescriptorInteger();
        final TypeDescriptorShortString valueDescriptor = new TypeDescriptorShortString();
        final IndexConfiguration<Integer, String> conf = IndexConfiguration
                .<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(keyDescriptor) //
                .withValueTypeDescriptor(valueDescriptor) //
                .withMaxNumberOfKeysInSegmentCache(8) //
                .withMaxNumberOfKeysInSegment(8) //
                .withMaxNumberOfKeysInSegmentChunk(4) //
                .withBloomFilterIndexSizeInBytes(1024 * 1024) //
                .withBloomFilterNumberOfHashFunctions(4) //
                .withContextLoggingEnabled(false) //
                .withName("metrics_test_index") //
                .build();

        try (SegmentIndex<Integer, String> index = SegmentIndex.create(directory,
                conf)) {
            index.put(1, "a");
            index.put(2, "b");
            assertEquals("a", index.get(1));
            assertNull(index.get(99));
            assertNull(index.get(100));
            assertNull(index.get(101));
            index.delete(2);

            final SegmentIndexMetricsSnapshot snapshot = index
                    .metricsSnapshot();
            assertEquals(2L, snapshot.getPutOperationCount());
            assertEquals(4L, snapshot.getGetOperationCount());
            assertEquals(1L, snapshot.getDeleteOperationCount());
            assertTrue(snapshot.getRegistryCacheHitCount() >= 1L);
            assertTrue(snapshot.getRegistryCacheMissCount() >= 1L);
            assertTrue(snapshot.getRegistryCacheLoadCount() >= 1L);
            assertTrue(snapshot.getRegistryCacheSize() >= 1);
            assertTrue(snapshot.getRegistryCacheLimit() >= 1);
            assertTrue(snapshot.getBloomFilterRequestCount() >= 0L);
            assertTrue(snapshot.getBloomFilterRefusedCount() >= 0L);
            assertTrue(snapshot.getBloomFilterPositiveCount() >= 0L);
            assertTrue(snapshot.getBloomFilterFalsePositiveCount() >= 0L);
            assertEquals(SegmentIndexState.READY, snapshot.getState());
        }
    }
}
