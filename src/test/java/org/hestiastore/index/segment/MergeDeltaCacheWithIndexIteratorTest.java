package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.junit.jupiter.api.Test;

class MergeDeltaCacheWithIndexIteratorTest extends AbstractSegmentTest {

    private final TypeDescriptorShortString tds = new TypeDescriptorShortString();
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    @Test
    void test_merge_simple() {
        final EntryIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(//
                        Entry.of("a", 10), //
                        Entry.of("b", 20), //
                        Entry.of("c", 30)),
                Arrays.asList(//
                        Entry.of("a", 11), //
                        Entry.of("b", 22), //
                        Entry.of("c", 33)));

        verifyIteratorData(Arrays.asList(//
                Entry.of("a", 11), //
                Entry.of("b", 22), //
                Entry.of("c", 33)//
        ), iterator);
    }

    @Test
    void test_merge_both_empty() {
        final EntryIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(), Arrays.asList());

        verifyIteratorData(Arrays.asList(), iterator);
    }

    @Test
    void test_move_to_invalid_item() {
        final EntryIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(), Arrays.asList());

        assertFalse(iterator.hasNext(), "Iterator should be empty");
        final Exception e = assertThrows(NoSuchElementException.class,
                () -> iterator.next());
        assertEquals("No next element.", e.getMessage());
    }

    @Test
    void test_merge_empty_indexFile() {
        final EntryIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(), Arrays.asList(//
                        Entry.of("a", 11), //
                        Entry.of("b", 22), //
                        Entry.of("c", 33)));

        verifyIteratorData(Arrays.asList(//
                Entry.of("a", 11), //
                Entry.of("b", 22), //
                Entry.of("c", 33)//
        ), iterator);
    }

    @Test
    void test_merge_empty_deltaCache() {
        final EntryIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(//
                        Entry.of("a", 10), //
                        Entry.of("b", 20), //
                        Entry.of("c", 30)),
                Arrays.asList());

        verifyIteratorData(Arrays.asList(//
                Entry.of("a", 10), //
                Entry.of("b", 20), //
                Entry.of("c", 30)//
        ), iterator);
    }

    @Test
    void test_merge_with_tombstone() {
        final EntryIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(//
                        Entry.of("a", 10), //
                        Entry.of("b", 20), //
                        Entry.of("c", 30)),
                Arrays.asList(//
                        Entry.of("a", 11), //
                        Entry.of("b", tdi.getTombstone()), //
                        Entry.of("e", 55)));

        verifyIteratorData(Arrays.asList(//
                Entry.of("a", 11), //
                Entry.of("c", 30), //
                Entry.of("e", 55)//
        ), iterator);
    }

    @Test
    void test_merge_with_tombstone_as_first_element() {
        final EntryIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(//
                        Entry.of("a", 10), //
                        Entry.of("b", 20), //
                        Entry.of("c", 30)),
                Arrays.asList(//
                        Entry.of("a", tdi.getTombstone()), //
                        Entry.of("b", 20), //
                        Entry.of("c", 55)));

        verifyIteratorData(Arrays.asList(//
                Entry.of("b", 20), //
                Entry.of("c", 55)//
        ), iterator);
    }

    @Test
    void test_merge_with_tombstone_fill_end_of_delata_cache() {
        final EntryIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(//
                        Entry.of("a", 10), //
                        Entry.of("b", 20), //
                        Entry.of("c", 30)),
                Arrays.asList(//
                        Entry.of("a", 11), //
                        Entry.of("b", tdi.getTombstone()), //
                        Entry.of("c", tdi.getTombstone()), //
                        Entry.of("e", tdi.getTombstone())));

        verifyIteratorData(Arrays.asList(//
                Entry.of("a", 11)//
        ), iterator);
    }

    @Test
    void test_merge_all_is_deleted() {
        final EntryIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(//
                        Entry.of("a", 10), //
                        Entry.of("b", 20), //
                        Entry.of("c", 30)), //
                Arrays.asList(//
                        Entry.of("a", tdi.getTombstone()), //
                        Entry.of("b", tdi.getTombstone()), //
                        Entry.of("c", tdi.getTombstone()), //
                        Entry.of("e", tdi.getTombstone())));

        verifyIteratorData(Arrays.asList(), iterator);
    }

    @Test
    void test_merge_all_is_deleted_2() {
        final EntryIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(), //
                Arrays.asList(//
                        Entry.of("a", tdi.getTombstone()), //
                        Entry.of("b", tdi.getTombstone()), //
                        Entry.of("c", tdi.getTombstone()), //
                        Entry.of("e", tdi.getTombstone())));

        verifyIteratorData(Arrays.asList(), iterator);
    }

    private MergeDeltaCacheWithIndexIterator<String, Integer> makeIterator(
            List<Entry<String, Integer>> indexFile,
            List<Entry<String, Integer>> deltaCache) {
        final EntryIteratorList<String, Integer> iteratorIndex = new EntryIteratorList<>(
                indexFile.iterator());
        return new MergeDeltaCacheWithIndexIterator<>(iteratorIndex, tds, tdi,
                deltaCache);
    }

}