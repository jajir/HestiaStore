package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.hestiastore.index.segmentregistry.BlockingSegment;
import org.hestiastore.index.segmentregistry.SegmentRegistry;
import org.junit.jupiter.api.Test;

class SegmentManagerTest {

    private final TypeDescriptor<Integer> keyTypeDescriptor = new TypeDescriptorInteger();

    private final TypeDescriptor<String> valueTypeDescriptor = new TypeDescriptorShortString();

    @Test
    void test_getting_same_segmentId() {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = testConfiguration();
        final ExecutorService stableSegmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        final SegmentRegistry<Integer, String> segmentRegistry = SegmentRegistry
                .<Integer, String>builder()
                .withDirectoryFacade(
                        directory)
                .withKeyTypeDescriptor(keyTypeDescriptor)
                .withValueTypeDescriptor(valueTypeDescriptor)
                .withConfiguration(conf)
                .withSegmentMaintenanceExecutor(stableSegmentMaintenancePool)
                .withRegistryMaintenanceExecutor(
                        Executors.newSingleThreadExecutor())
                .build();

        final BlockingSegment<Integer, String> s1 = segmentRegistry
                .createSegment();
        assertNotNull(s1);
        final SegmentId segmentId = s1.getId();

        final BlockingSegment<Integer, String> s2 = segmentRegistry
                .loadSegment(segmentId);
        assertNotNull(s1);

        /*
         * Verify that first object was cached and second time just returned
         * from map.
         */
        assertSame(s1, s2);
        segmentRegistry.close();
        stableSegmentMaintenancePool.shutdownNow();
    }

    @Test
    void test_close() {
        final Directory directory = new MemDirectory();
        final IndexConfiguration<Integer, String> conf = testConfiguration();
        final ExecutorService stableSegmentMaintenancePool = Executors
                .newSingleThreadExecutor();
        final SegmentRegistry<Integer, String> segmentRegistry = SegmentRegistry
                .<Integer, String>builder()
                .withDirectoryFacade(
                        directory)
                .withKeyTypeDescriptor(keyTypeDescriptor)
                .withValueTypeDescriptor(valueTypeDescriptor)
                .withConfiguration(conf)
                .withSegmentMaintenanceExecutor(stableSegmentMaintenancePool)
                .withRegistryMaintenanceExecutor(
                        Executors.newSingleThreadExecutor())
                .build();
        assertDoesNotThrow(() -> segmentRegistry.close());
        stableSegmentMaintenancePool.shutdownNow();
    }

    private static IndexConfiguration<Integer, String> testConfiguration() {
        return IndexConfiguration.<Integer, String>builder()
                .segment(segment -> segment.cachedSegmentLimit(3)
                        .cacheKeyLimit(4).chunkKeyLimit(1)
                        .deltaCacheFileLimit(3))
                .writePath(writePath -> writePath.segmentWriteCacheKeyLimit(1)
                        .maintenanceWriteCacheKeyLimit(2))
                .io(io -> io.diskBufferSizeBytes(1024))
                .bloomFilter(bloomFilter -> bloomFilter.hashFunctions(1)
                        .indexSizeBytes(0)
                        .falsePositiveProbability(0.01))
                .filters(filters -> filters
                        .encodingFilters(List.of(new ChunkFilterDoNothing()))
                        .decodingFilters(List.of(new ChunkFilterDoNothing())))
                .build();
    }

}
