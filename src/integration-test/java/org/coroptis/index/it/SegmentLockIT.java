package org.coroptis.index.it;

import static org.hestiastore.index.segment.SegmentTestHelper.closeAndAwait;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.hestiastore.index.segment.Segment;
import org.hestiastore.index.segment.SegmentBuildStatus;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class SegmentLockIT {

    @Test
    void inMemoryLockPreventsDoubleOpen() {
        final Directory directory = new MemDirectory();
        final SegmentId segmentId = SegmentId.of(1);
        final Segment<Integer, String> first = newSegment(directory, segmentId);
        try {
            assertEquals(SegmentBuildStatus.BUSY,
                    Segment.<Integer, String>builder(
                            AsyncDirectoryAdapter.wrap(directory))//
                            .withId(segmentId)//
                            .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                            .withValueTypeDescriptor(
                                    new TypeDescriptorShortString())//
                            .withMaxNumberOfKeysInSegmentWriteCache(4)//
                            .withMaxNumberOfKeysInSegmentCache(8)//
                            .withMaxNumberOfKeysInSegmentChunk(2)//
                            .withBloomFilterIndexSizeInBytes(0)//
                            .withEncodingChunkFilters(
                                    List.of(new ChunkFilterDoNothing()))//
                            .withDecodingChunkFilters(
                                    List.of(new ChunkFilterDoNothing()))//
                            .build().getStatus());
        } finally {
            closeAndAwait(first);
        }

        final Segment<Integer, String> reopened = newSegment(directory,
                segmentId);
        closeAndAwait(reopened);
    }

    private Segment<Integer, String> newSegment(final Directory directory,
            final SegmentId segmentId) {
        return Segment.<Integer, String>builder(
                AsyncDirectoryAdapter.wrap(directory))//
                .withId(segmentId)//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withMaxNumberOfKeysInSegmentWriteCache(4)//
                .withMaxNumberOfKeysInSegmentCache(8)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .build().getValue();
    }
}
