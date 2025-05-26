package org.hestiastore.index.segment;

import java.util.Arrays;
import java.util.List;

import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIterator;
import org.hestiastore.index.PairIteratorList;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.segment.MergeDeltaCacheWithIndexIterator;
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

        verifyIteratorData(iterator, Arrays.asList(//
                Pair.of("a", 11), //
                Pair.of("b", 22), //
                Pair.of("c", 33)));
    }

    @Test
    void test_merge_both_empty() {
        final PairIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(), Arrays.asList());

        verifyIteratorData(iterator, Arrays.asList());
    }

    @Test
    void test_merge_empty_indexFile() {
        final PairIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(), Arrays.asList(//
                        Pair.of("a", 11), //
                        Pair.of("b", 22), //
                        Pair.of("c", 33)));

        verifyIteratorData(iterator, Arrays.asList(//
                Pair.of("a", 11), //
                Pair.of("b", 22), //
                Pair.of("c", 33)));
    }

    @Test
    void test_merge_empty_deltaCache() {
        final PairIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(//
                        Pair.of("a", 10), //
                        Pair.of("b", 20), //
                        Pair.of("c", 30)),
                Arrays.asList());

        verifyIteratorData(iterator, Arrays.asList(//
                Pair.of("a", 10), //
                Pair.of("b", 20), //
                Pair.of("c", 30)));
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

        verifyIteratorData(iterator, Arrays.asList(//
                Pair.of("a", 11), //
                Pair.of("c", 30), //
                Pair.of("e", 55)));
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

        verifyIteratorData(iterator, Arrays.asList(//
                Pair.of("a", 11)//
        ));
    }

    @Test
    void test_merge_all_is_deleted() {
        final PairIterator<String, Integer> iterator = makeIterator(//
                Arrays.asList(//
                        Pair.of("a", 10), //
                        Pair.of("b", 20), //
                        Pair.of("c", 30)),
                Arrays.asList(//
                        Pair.of("a", tdi.getTombstone()), //
                        Pair.of("b", tdi.getTombstone()), //
                        Pair.of("c", tdi.getTombstone()), //
                        Pair.of("e", tdi.getTombstone())));

        verifyIteratorData(iterator, Arrays.asList());
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