package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.directory.async.AsyncDirectoryAdapter;
import org.junit.jupiter.api.Test;

class SegmentImplConcurrencyContractTest {

    @Test
    void exclusiveAccess_blocks_operations_until_closed() {
        try (Segment<Integer, String> segment = newSegment(2)) {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "a").getStatus());
            final SegmentResult<EntryIterator<Integer, String>> exclusive = segment
                    .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
            assertEquals(SegmentResultStatus.OK, exclusive.getStatus());

            assertEquals(SegmentResultStatus.BUSY,
                    segment.put(2, "b").getStatus());
            assertEquals(SegmentResultStatus.BUSY, segment.get(1).getStatus());
            assertEquals(SegmentResultStatus.BUSY,
                    segment.flush().getStatus());
            assertEquals(SegmentResultStatus.BUSY,
                    segment.compact().getStatus());
            assertEquals(SegmentResultStatus.BUSY,
                    segment.openIterator().getStatus());

            exclusive.getValue().close();

            assertEquals(SegmentResultStatus.OK,
                    segment.put(2, "b").getStatus());
            assertEquals(SegmentResultStatus.OK, segment.get(1).getStatus());
        }
    }

    @Test
    void exclusiveAccess_invalidates_failFast_iterators() {
        try (Segment<Integer, String> segment = newSegment(2)) {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "a").getStatus());
            assertEquals(SegmentResultStatus.OK,
                    segment.put(2, "b").getStatus());
            final SegmentResult<EntryIterator<Integer, String>> failFastResult = segment
                    .openIterator();
            assertEquals(SegmentResultStatus.OK, failFastResult.getStatus());
            final EntryIterator<Integer, String> failFast = failFastResult
                    .getValue();
            assertTrue(failFast.hasNext());

            final SegmentResult<EntryIterator<Integer, String>> exclusive = segment
                    .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
            assertEquals(SegmentResultStatus.OK, exclusive.getStatus());

            assertFalse(failFast.hasNext());

            exclusive.getValue().close();
            failFast.close();
        }
    }

    @Test
    void flush_invalidates_failFast_iterators() {
        try (Segment<Integer, String> segment = newSegment(2)) {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "a").getStatus());
            final SegmentResult<EntryIterator<Integer, String>> failFastResult = segment
                    .openIterator();
            assertEquals(SegmentResultStatus.OK, failFastResult.getStatus());
            final EntryIterator<Integer, String> failFast = failFastResult
                    .getValue();
            assertTrue(failFast.hasNext());

            assertEquals(SegmentResultStatus.OK,
                    segment.flush().getStatus());

            assertFalse(failFast.hasNext());
            failFast.close();
        }
    }

    @Test
    void put_returns_busy_when_write_cache_full() {
        try (Segment<Integer, String> segment = newSegment(1)) {
            assertEquals(SegmentResultStatus.OK,
                    segment.put(1, "a").getStatus());
            assertEquals(SegmentResultStatus.BUSY,
                    segment.put(2, "b").getStatus());
        }
    }

    private Segment<Integer, String> newSegment(final int writeCacheSize) {
        return Segment.<Integer, String>builder()//
                .withAsyncDirectory(AsyncDirectoryAdapter.wrap(new MemDirectory()))//
                .withId(SegmentId.of(1))//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorShortString())//
                .withMaxNumberOfKeysInSegmentWriteCache(writeCacheSize)//
                .withMaxNumberOfKeysInSegmentCache(8)//
                .withMaxNumberOfKeysInSegmentChunk(2)//
                .withBloomFilterIndexSizeInBytes(0)//
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()))//
                .build();
    }
}
