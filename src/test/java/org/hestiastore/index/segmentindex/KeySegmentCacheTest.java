package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.directory.MemDirectory;
import org.hestiastore.index.segment.SegmentId;
import org.junit.jupiter.api.Test;

class KeySegmentCacheTest {

    @Test
    void findNewSegmentIdUsesMaxPlusOne() {
        final KeySegmentCache<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(0)), Entry.of(20, SegmentId.of(2))));
        assertEquals(SegmentId.of(3), cache.findNewSegmentId());
    }

    @Test
    void findNewSegmentIdUsesHighestIdNotKeyOrder() {
        final KeySegmentCache<Integer> cache = newCacheWithEntries(List.of(
                Entry.of(10, SegmentId.of(5)), Entry.of(20, SegmentId.of(1))));
        // highest id is 5 even though its key is lower
        assertEquals(SegmentId.of(6), cache.findNewSegmentId());
    }

    @Test
    void findNewSegmentIdBridgesGaps() {
        final KeySegmentCache<Integer> cache = newCacheWithEntries(
                List.of(Entry.of(5, SegmentId.of(1)),
                        Entry.of(6, SegmentId.of(3))));
        assertEquals(SegmentId.of(4), cache.findNewSegmentId());
    }

    @Test
    void findNewSegmentIdStartsAtZeroWhenEmpty() {
        final KeySegmentCache<Integer> cache = newCacheWithEntries(List.of());
        assertEquals(SegmentId.of(0), cache.findNewSegmentId());
    }

    @Test
    void insertSegmentRejectsDuplicateId() {
        final KeySegmentCache<Integer> cache = newCacheWithEntries(List.of());
        cache.insertSegment(5, SegmentId.of(0));
        assertThrows(IllegalArgumentException.class,
                () -> cache.insertSegment(6, SegmentId.of(0)));
    }

    private KeySegmentCache<Integer> newCacheWithEntries(
            final List<Entry<Integer, SegmentId>> entries) {
        final MemDirectory dir = new MemDirectory();
        final var sdf = org.hestiastore.index.sorteddatafile.SortedDataFile
                .<Integer, SegmentId>builder()//
                .withDirectory(dir)//
                .withFileName("index.map")//
                .withKeyTypeDescriptor(new TypeDescriptorInteger())//
                .withValueTypeDescriptor(new TypeDescriptorSegmentId())//
                .build();
        // seed file contents
        sdf.openWriterTx().execute(writer -> entries.stream()
                .sorted((e1, e2) -> Integer.compare(e1.getKey(), e2.getKey()))
                .forEach(writer::write));
        return new KeySegmentCache<>(dir, new TypeDescriptorInteger());
    }
}
