package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.Executors;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentManagerTest {

    private final TypeDescriptor<Integer> keyTypeDescriptor = new TypeDescriptorInteger();

    private final TypeDescriptor<String> valueTypeDescriptor = new TypeDescriptorShortString();

    @Mock
    private IndexConfiguration<Integer, String> conf;

    @Test
    void test_getting_same_segmentId() {
        final Directory directory = new MemDirectory();
        when(conf.getMaxNumberOfSegmentsInCache()).thenReturn(3);
        final SegmentAsyncExecutor maintenanceExecutor = new SegmentAsyncExecutor(
                1, "segment-maintenance");
        final SegmentRegistry<Integer, String> segmentRegistry = SegmentRegistry
                .<Integer, String>builder()
                .withDirectoryFacade(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))
                .withKeyTypeDescriptor(keyTypeDescriptor)
                .withValueTypeDescriptor(valueTypeDescriptor)
                .withConfiguration(conf)
                .withMaintenanceExecutor(maintenanceExecutor.getExecutor())
                .withLifecycleExecutor(Executors.newSingleThreadExecutor())
                .build();
        when(conf.getMaxNumberOfKeysInSegmentWriteCache()).thenReturn(1);
        when(conf.getMaxNumberOfKeysInSegmentWriteCacheDuringMaintenance())
                .thenReturn(2);
        when(conf.getMaxNumberOfKeysInSegmentCache()).thenReturn(4);
        when(conf.getMaxNumberOfKeysInSegmentChunk()).thenReturn(1);
        when(conf.getMaxNumberOfDeltaCacheFiles()).thenReturn(3);
        when(conf.getDiskIoBufferSize()).thenReturn(1024);
        when(conf.getBloomFilterNumberOfHashFunctions()).thenReturn(1);
        when(conf.getBloomFilterIndexSizeInBytes()).thenReturn(0);
        when(conf.getBloomFilterProbabilityOfFalsePositive()).thenReturn(0.01);
        when(conf.getEncodingChunkFilters())
                .thenReturn(List.of(new ChunkFilterDoNothing()));
        when(conf.getDecodingChunkFilters())
                .thenReturn(List.of(new ChunkFilterDoNothing()));

        final Segment<Integer, String> s1 = segmentRegistry
                .getSegment(SegmentId.of(1)).getValue();
        assertNotNull(s1);

        final Segment<Integer, String> s2 = segmentRegistry
                .getSegment(SegmentId.of(1)).getValue();
        assertNotNull(s1);

        /*
         * Verify that first object was cached and second time just returned
         * from map.
         */
        assertSame(s1, s2);
        segmentRegistry.close();
        if (!maintenanceExecutor.wasClosed()) {
            maintenanceExecutor.close();
        }
    }

    @Test
    void test_close() {
        final Directory directory = new MemDirectory();
        when(conf.getMaxNumberOfSegmentsInCache()).thenReturn(3);
        final SegmentAsyncExecutor maintenanceExecutor = new SegmentAsyncExecutor(
                1, "segment-maintenance");
        final SegmentRegistry<Integer, String> segmentRegistry = SegmentRegistry
                .<Integer, String>builder()
                .withDirectoryFacade(
                        org.hestiastore.index.directory.async.AsyncDirectoryAdapter
                                .wrap(directory))
                .withKeyTypeDescriptor(keyTypeDescriptor)
                .withValueTypeDescriptor(valueTypeDescriptor)
                .withConfiguration(conf)
                .withMaintenanceExecutor(maintenanceExecutor.getExecutor())
                .withLifecycleExecutor(Executors.newSingleThreadExecutor())
                .build();
        assertDoesNotThrow(() -> segmentRegistry.close());
        if (!maintenanceExecutor.wasClosed()) {
            maintenanceExecutor.close();
        }
    }

}
