package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.coroptis.index.PairIterator;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.Directory;
import com.coroptis.index.directory.MemDirectory;

public class SegmentIteratorTest {

    private final TypeDescriptorString tds = new TypeDescriptorString();
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    @Test
    void test_iteration_from_sstFile() throws Exception {
        final Directory directory = new MemDirectory();
        final SegmentId id = SegmentId.of(27);
        final Segment<Integer, String> segment = Segment
                .<Integer, String>builder().withDirectory(directory).withId(id)
                .withKeyTypeDescriptor(tdi).withValueTypeDescriptor(tds)
                .withMaxNumberOfKeysInSegmentCache(5)
                .withMaxNumberOfKeysInIndexPage(3).build();
        try (final SegmentWriter<Integer, String> writer = segment
                .openWriter()) {
            writer.put(1, "a");
            writer.put(2, "b");
            writer.put(3, "c");
        }

        try (PairIterator<Integer, String> iterator = segment.openIterator()) {
            assertTrue(iterator.readCurrent().isEmpty());
            assertTrue(iterator.hasNext());
            iterator.next();
            try (final SegmentWriter<Integer, String> writer = segment
                    .openWriter()) {
                writer.put(4, "d");
                writer.put(5, "e");
            }
            segment.forceCompact();
            assertTrue(iterator.readCurrent().isPresent());
            /**
             * There should not be next element, because operation forceCompact
             * force index to rewrite physical SST data file.
             */
            assertFalse(iterator.hasNext());
        }
    }

    // TODO add test that stop iterating over segment when is split.

}
