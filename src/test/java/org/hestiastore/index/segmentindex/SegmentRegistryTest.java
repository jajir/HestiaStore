package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentRegistryTest {

    @Test
    void getSegment_reusesInstanceUntilClosed() {
        final SegmentRegistry<Integer, String> registry = new SegmentRegistry<>(
                AsyncDirectoryAdapter.wrap(new MemDirectory()),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                buildConf());
        final SegmentId segmentId = SegmentId.of(1);

        final Segment<Integer, String> first = registry.getSegment(segmentId);
        final Segment<Integer, String> second = registry.getSegment(segmentId);

        assertSame(first, second);

        first.close();
        final Segment<Integer, String> third = registry.getSegment(segmentId);

        assertNotSame(first, third);
    }

    @Test
    void evictSegmentIfSame_removesOnlyMatchingInstance() {
        final SegmentRegistry<Integer, String> registry = new SegmentRegistry<>(
                AsyncDirectoryAdapter.wrap(new MemDirectory()),
                new TypeDescriptorInteger(), new TypeDescriptorShortString(),
                buildConf());
        final SegmentId segmentId = SegmentId.of(1);
        final SegmentId otherId = SegmentId.of(2);

        final Segment<Integer, String> segment = registry.getSegment(segmentId);
        final Segment<Integer, String> otherSegment = registry
                .getSegment(otherId);

        assertFalse(registry.evictSegmentIfSame(segmentId, otherSegment));
        assertSame(segment, registry.getSegment(segmentId));

        assertTrue(registry.evictSegmentIfSame(segmentId, segment));
        assertNotSame(segment, registry.getSegment(segmentId));
    }

    private IndexConfiguration<Integer, String> buildConf() {
        return IndexConfiguration.<Integer, String>builder()
                .withKeyClass(Integer.class)
                .withValueClass(String.class)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withName("segment-registry-test")
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
