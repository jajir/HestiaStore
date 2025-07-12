package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.AbstractDataTest;
import org.hestiastore.index.Pair;
import org.hestiastore.index.PairIteratorList;
import org.hestiastore.index.PairIteratorWithCurrent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MergedPairIteratorTest extends AbstractDataTest {

    private static final Comparator<String> KEY_COMPARATOR = Comparator
            .naturalOrder();

    private static final Merger<String, Integer> MERGER = (k, v1, v2) -> v1;

    private static final Pair<String, Integer> PAIR1 = new Pair<>("a", 1);
    private static final Pair<String, Integer> PAIR2 = new Pair<>("b", 2);
    private static final Pair<String, Integer> PAIR3 = new Pair<>("c", 3);
    private static final Pair<String, Integer> PAIR4 = new Pair<>("d", 4);
    private static final Pair<String, Integer> PAIR5 = new Pair<>("e", 5);

    private PairIteratorWithCurrent<String, Integer> iterator1;

    private PairIteratorWithCurrent<String, Integer> iterator2;

    private PairIteratorWithCurrent<String, Integer> iterator3;

    private List<PairIteratorWithCurrent<String, Integer>> iterators;

    @BeforeEach
    void setUp() {
        iterator1 = new PairIteratorList<>(Arrays.asList(PAIR1, PAIR2, PAIR3));
        iterator2 = new PairIteratorList<>(Arrays.asList(PAIR2, PAIR3, PAIR4));
        iterator3 = new PairIteratorList<>(Arrays.asList(PAIR3, PAIR4, PAIR5));
        iterators = List.of(iterator1, iterator2, iterator3);
    }

    @AfterEach
    void tearDown() {
        iterators = null;
    }

    @Test
    void test_constructor_missingIterators() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new MergedPairIterator<>(null, KEY_COMPARATOR, MERGER));

        assertEquals("Property 'iterators' must not be null.", e.getMessage());
    }

    @Test
    void test_constructor_missingKeyComparator() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new MergedPairIterator<>(iterators, null, MERGER));

        assertEquals("Property 'keyComparator' must not be null.",
                e.getMessage());
    }

    @Test
    void test_constructor_missingMerger() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new MergedPairIterator<>(iterators, KEY_COMPARATOR,
                        null));

        assertEquals("Property 'merger' must not be null.", e.getMessage());
    }

    @Test
    void test_no_iterator() {
        MergedPairIterator<String, Integer> iterator = new MergedPairIterator<>(
                Collections.emptyList(), KEY_COMPARATOR, MERGER);

        verifyIteratorData(Collections.emptyList(), iterator);
    }

    @Test
    void test_one_iterator() {
        MergedPairIterator<String, Integer> iterator = new MergedPairIterator<>(
                Arrays.asList(iterator1), KEY_COMPARATOR, MERGER);

        verifyIteratorData(Arrays.asList(PAIR1, PAIR2, PAIR3), iterator);
    }

    @Test
    void test_two_iterators() {
        MergedPairIterator<String, Integer> iterator = new MergedPairIterator<>(
                Arrays.asList(iterator1, iterator2), KEY_COMPARATOR, MERGER);

        verifyIteratorData(Arrays.asList(PAIR1, PAIR2, PAIR3, PAIR4), iterator);
    }

    @Test
    void test_three_iterators() {
        MergedPairIterator<String, Integer> iterator = new MergedPairIterator<>(
                iterators, KEY_COMPARATOR, MERGER);

        verifyIteratorData(Arrays.asList(PAIR1, PAIR2, PAIR3, PAIR4, PAIR5),
                iterator);
    }

    @Test
    void test_two_different_length() {
        MergedPairIterator<String, Integer> iterator = new MergedPairIterator<>(
                List.of(new PairIteratorList<>(Arrays.asList(PAIR1, PAIR2)), //
                        new PairIteratorList<>(Arrays.asList(PAIR3))), //
                KEY_COMPARATOR, MERGER);

        verifyIteratorData(Arrays.asList(PAIR1, PAIR2, PAIR3), iterator);
    }

}
