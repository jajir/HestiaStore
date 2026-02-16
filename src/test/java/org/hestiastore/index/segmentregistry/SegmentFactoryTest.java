package org.hestiastore.index.segmentregistry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentBuildResult;
import org.hestiastore.index.segment.SegmentBuildStatus;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segment.SegmentTestHelper;
import org.hestiastore.index.segmentindex.IndexConfiguration;
import org.hestiastore.index.segmentindex.SegmentAsyncExecutor;
import org.junit.jupiter.api.Test;

class SegmentFactoryTest {

    @Test
    void buildSegment_createsSegmentWithId() {
        final IndexConfiguration<Integer, String> conf = newConfiguration();
        final Directory directory = new MemDirectory();
        final SegmentAsyncExecutor maintenanceExecutor = new SegmentAsyncExecutor(
                1, "segment-maintenance-test");
        final SegmentFactory<Integer, String> factory = new SegmentFactory<>(
                directory, new TypeDescriptorInteger(),
                new TypeDescriptorShortString(), conf,
                maintenanceExecutor.getExecutor());
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentBuildResult<Segment<Integer, String>> buildResult = factory
                .buildSegment(segmentId);
        final Segment<Integer, String> segment = buildResult.getValue();
        try {
            assertEquals(SegmentBuildStatus.OK, buildResult.getStatus());
            assertNotNull(segment);
            assertEquals(segmentId, segment.getId());
        } finally {
            SegmentTestHelper.closeAndAwait(segment);
            if (!maintenanceExecutor.wasClosed()) {
                maintenanceExecutor.close();
            }
        }
    }

    private static IndexConfiguration<Integer, String> newConfiguration() {
        return IndexConfiguration.<Integer, String>builder()//
                .withKeyClass(Integer.class)//
                .withValueClass(String.class)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withMaxNumberOfKeysInSegmentCache(10)//
                .withMaxNumberOfKeysInSegmentWriteCache(5)//
                .withMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance(10)//
                .withMaxNumberOfKeysInSegmentChunk(4)//
                .withMaxNumberOfDeltaCacheFiles(2)//
                .withMaxNumberOfKeysInCache(100)//
                .withMaxNumberOfKeysInSegment(50)//
                .withMaxNumberOfSegmentsInCache(5)//
                .withBloomFilterNumberOfHashFunctions(1)//
                .withBloomFilterIndexSizeInBytes(128)//
                .withBloomFilterProbabilityOfFalsePositive(0.01)//
                .withDiskIoBufferSizeInBytes(1024)//
                .withEncodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingFilters(List.of(new ChunkFilterDoNothing()))//
                .withSegmentMaintenanceAutoEnabled(false)//
                .withIndexWorkerThreadCount(1)//
                .withNumberOfIoThreads(1)//
                .withNumberOfSegmentIndexMaintenanceThreads(1)//
                .withNumberOfIndexMaintenanceThreads(1)//
                .withIndexBusyBackoffMillis(1)//
                .withIndexBusyTimeoutMillis(1000)//
                .withContextLoggingEnabled(false)//
                .withName("segment-factory-test")//
                .build();
    }
}
