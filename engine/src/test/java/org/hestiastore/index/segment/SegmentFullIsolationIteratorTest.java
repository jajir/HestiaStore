package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.chunkstore.ChunkFilterDoNothing;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class SegmentFullIsolationIteratorTest {

    @Test
    void fullIsolationIterator_reads_index_delta_and_write_cache() {
        final MemDirectory directory = new MemDirectory();
        final Directory asyncDirectory = directory;
        final SegmentId segmentId = SegmentId.of(1);

        final SegmentBuilder<Integer, String> builder = newBuilder(
                asyncDirectory, segmentId);
        builder.openWriterTx().execute(writer -> {
            writer.write(Entry.of(1, "index-1"));
            writer.write(Entry.of(2, "index-2"));
        });

        final Segment<Integer, String> segment = builder.build().getValue();
        try {
            // write-cache entries that will be flushed into delta-cache files
            segment.put(3, "delta-3");
            segment.put(4, "delta-4");
            assertEquals(SegmentResultStatus.OK, segment.flush().getStatus());
            assertEquals(0, segment.getNumberOfKeysInWriteCache());

            // keep some entries in the write cache only
            segment.put(5, "write-5");
            segment.put(6, "write-6");
            assertEquals(2, segment.getNumberOfKeysInWriteCache());

            final SegmentResult<EntryIterator<Integer, String>> iteratorResult = segment
                    .openIterator(SegmentIteratorIsolation.FULL_ISOLATION);
            assertEquals(SegmentResultStatus.OK, iteratorResult.getStatus());
            assertNotNull(iteratorResult.getValue());

            final List<Integer> keys = new ArrayList<>();
            try (EntryIterator<Integer, String> iterator = iteratorResult
                    .getValue()) {
                while (iterator.hasNext()) {
                    keys.add(iterator.next().getKey());
                }
            }
            keys.sort(Integer::compareTo);

            assertEquals(List.of(1, 2, 3, 4, 5, 6), keys);
        } finally {
            segment.close();
        }
    }

    private SegmentBuilder<Integer, String> newBuilder(
            final Directory asyncDirectory, final SegmentId segmentId) {
        return Segment.<Integer, String>builder(asyncDirectory)
                .withId(segmentId)
                .withKeyTypeDescriptor(new TypeDescriptorInteger())
                .withValueTypeDescriptor(new TypeDescriptorShortString())
                .withMaxNumberOfKeysInSegmentWriteCache(10)
                .withMaxNumberOfKeysInSegmentCache(20)
                .withMaxNumberOfKeysInSegmentChunk(2)
                .withBloomFilterIndexSizeInBytes(0)
                .withMaintenancePolicy(SegmentMaintenancePolicy.none())
                .withEncodingChunkFilters(List.of(new ChunkFilterDoNothing()))
                .withDecodingChunkFilters(List.of(new ChunkFilterDoNothing()));
    }
}
