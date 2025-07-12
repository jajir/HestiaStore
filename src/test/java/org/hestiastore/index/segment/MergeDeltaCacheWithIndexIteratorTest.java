package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairIteratorList;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.junit.jupiter.api.Test;

class MergeDeltaCacheWithIndexIteratorTest extends AbstractSegmentTest {

    private final TypeDescriptorString tds = new TypeDescriptorString();
    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    @Test
    void test_merge_simple() {
        final PairIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(//
                        Pair.of("a", 10), //
                        Pair.of("b", 20), //
                        Pair.of("c", 30)),
                Arrays.asList(//
                        Pair.of("a", 11), //
                        Pair.of("b", 22), //
                        Pair.of("c", 33)));

        verifyIteratorData(Arrays.asList(//
                Pair.of("a", 11), //
                Pair.of("b", 22), //
                Pair.of("c", 33)//
        ), iterator);
    }

    @Test
    void test_merge_both_empty() {
        final PairIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(), Arrays.asList());

        verifyIteratorData(Arrays.asList(), iterator);
    }

    @Test
    void test_move_to_invalid_item() {
        final PairIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(), Arrays.asList());

        assertFalse(iterator.hasNext(), "Iterator should be empty");
        final Exception e = assertThrows(NoSuchElementException.class,
                () -> iterator.next());
        assertEquals("No next element.", e.getMessage());
    }

    @Test
    void test_merge_empty_indexFile() {
        final PairIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(), Arrays.asList(//
                        Pair.of("a", 11), //
                        Pair.of("b", 22), //
                        Pair.of("c", 33)));

        verifyIteratorData(Arrays.asList(//
                Pair.of("a", 11), //
                Pair.of("b", 22), //
                Pair.of("c", 33)//
        ), iterator);
    }

    @Test
    void test_merge_empty_deltaCache() {
        final PairIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(//
                        Pair.of("a", 10), //
                        Pair.of("b", 20), //
                        Pair.of("c", 30)),
                Arrays.asList());

        verifyIteratorData(Arrays.asList(//
                Pair.of("a", 10), //
                Pair.of("b", 20), //
                Pair.of("c", 30)//
        ), iterator);
    }

    @Test
    void test_merge_with_tombstone() {
        final PairIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(//
                        Pair.of("a", 10), //
                        Pair.of("b", 20), //
                        Pair.of("c", 30)),
                Arrays.asList(//
                        Pair.of("a", 11), //
                        Pair.of("b", tdi.getTombstone()), //
                        Pair.of("e", 55)));

        verifyIteratorData(Arrays.asList(//
                Pair.of("a", 11), //
                Pair.of("c", 30), //
                Pair.of("e", 55)//
        ), iterator);
    }

    @Test
    void test_merge_with_tombstone_as_first_element() {
        final PairIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(//
                        Pair.of("a", 10), //
                        Pair.of("b", 20), //
                        Pair.of("c", 30)),
                Arrays.asList(//
                        Pair.of("a", tdi.getTombstone()), //
                        Pair.of("b", 20), //
                        Pair.of("c", 55)));

        verifyIteratorData(Arrays.asList(//
                Pair.of("b", 20), //
                Pair.of("c", 55)//
        ), iterator);
    }

    @Test
    void test_merge_with_tombstone_fill_end_of_delata_cache() {
        final PairIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(//
                        Pair.of("a", 10), //
                        Pair.of("b", 20), //
                        Pair.of("c", 30)),
                Arrays.asList(//
                        Pair.of("a", 11), //
                        Pair.of("b", tdi.getTombstone()), //
                        Pair.of("c", tdi.getTombstone()), //
                        Pair.of("e", tdi.getTombstone())));

        verifyIteratorData(Arrays.asList(//
                Pair.of("a", 11)//
        ), iterator);
    }

    @Test
    void test_merge_all_is_deleted() {
        final PairIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(//
                        Pair.of("a", 10), //
                        Pair.of("b", 20), //
                        Pair.of("c", 30)), //
                Arrays.asList(//
                        Pair.of("a", tdi.getTombstone()), //
                        Pair.of("b", tdi.getTombstone()), //
                        Pair.of("c", tdi.getTombstone()), //
                        Pair.of("e", tdi.getTombstone())));

        verifyIteratorData(Arrays.asList(), iterator);
    }

    @Test
    void test_merge_all_is_deleted_2() {
        final PairIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(), //
                Arrays.asList(//
                        Pair.of("a", tdi.getTombstone()), //
                        Pair.of("b", tdi.getTombstone()), //
                        Pair.of("c", tdi.getTombstone()), //
                        Pair.of("e", tdi.getTombstone())));

        verifyIteratorData(Arrays.asList(), iterator);
    }

    private MergeDeltaCacheWithIndexIterator<String, Integer> makeIterator(
            List<Pair<String, Integer>> indexFile,
            List<Pair<String, Integer>> deltaCache) {
        final PairIteratorList<String, Integer> iteratorIndex = new PairIteratorList<>(
                indexFile.iterator());
        return new MergeDeltaCacheWithIndexIterator<>(iteratorIndex, tds, tdi,
                deltaCache);
    }

}