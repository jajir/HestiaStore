package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.AbstractDataTest;
import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MergedEntryIteratorTest extends AbstractDataTest {

    private static final Comparator<String> KEY_COMPARATOR = Comparator
            .naturalOrder();

    private static final Merger<String, Integer> MERGER = (k, v1, v2) -> v1;

    private static final Entry<String, Integer> ENTRY1 = new Entry<>("a", 1);
    private static final Entry<String, Integer> ENTRY2 = new Entry<>("b", 2);
    private static final Entry<String, Integer> ENTRY3 = new Entry<>("c", 3);
    private static final Entry<String, Integer> ENTRY4 = new Entry<>("d", 4);
    private static final Entry<String, Integer> ENTRY5 = new Entry<>("e", 5);

    private EntryIteratorWithCurrent<String, Integer> iterator1;

    private EntryIteratorWithCurrent<String, Integer> iterator2;

    private EntryIteratorWithCurrent<String, Integer> iterator3;

    private List<EntryIteratorWithCurrent<String, Integer>> iterators;

    @BeforeEach
    void setUp() {
        iterator1 = new EntryIteratorList<>(Arrays.asList(ENTRY1, ENTRY2, ENTRY3));
        iterator2 = new EntryIteratorList<>(Arrays.asList(ENTRY2, ENTRY3, ENTRY4));
        iterator3 = new EntryIteratorList<>(Arrays.asList(ENTRY3, ENTRY4, ENTRY5));
        iterators = List.of(iterator1, iterator2, iterator3);
    }

    @AfterEach
    void tearDown() {
        iterators = null;
    }

    @Test
    void test_constructor_missingIterators() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new MergedEntryIterator<>(null, KEY_COMPARATOR, MERGER));

        assertEquals("Property 'iterators' must not be null.", e.getMessage());
    }

    @Test
    void test_constructor_missingKeyComparator() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new MergedEntryIterator<>(iterators, null, MERGER));

        assertEquals("Property 'keyComparator' must not be null.",
                e.getMessage());
    }

    @Test
    void test_constructor_missingMerger() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new MergedEntryIterator<>(iterators, KEY_COMPARATOR,
                        null));

        assertEquals("Property 'merger' must not be null.", e.getMessage());
    }

    @Test
    void test_no_iterator() {
        MergedEntryIterator<String, Integer> iterator = new MergedEntryIterator<>(
                Collections.emptyList(), KEY_COMPARATOR, MERGER);

        verifyIteratorData(Collections.emptyList(), iterator);
    }

    @Test
    void test_one_iterator() {
        MergedEntryIterator<String, Integer> iterator = new MergedEntryIterator<>(
                Arrays.asList(iterator1), KEY_COMPARATOR, MERGER);

        verifyIteratorData(Arrays.asList(ENTRY1, ENTRY2, ENTRY3), iterator);
    }

    @Test
    void test_two_iterators() {
        MergedEntryIterator<String, Integer> iterator = new MergedEntryIterator<>(
                Arrays.asList(iterator1, iterator2), KEY_COMPARATOR, MERGER);

        verifyIteratorData(Arrays.asList(ENTRY1, ENTRY2, ENTRY3, ENTRY4), iterator);
    }

    @Test
    void test_three_iterators() {
        MergedEntryIterator<String, Integer> iterator = new MergedEntryIterator<>(
                iterators, KEY_COMPARATOR, MERGER);

        verifyIteratorData(Arrays.asList(ENTRY1, ENTRY2, ENTRY3, ENTRY4, ENTRY5),
                iterator);
    }

    @Test
    void test_two_different_length() {
        MergedEntryIterator<String, Integer> iterator = new MergedEntryIterator<>(
                List.of(new EntryIteratorList<>(Arrays.asList(ENTRY1, ENTRY2)), //
                        new EntryIteratorList<>(Arrays.asList(ENTRY3))), //
                KEY_COMPARATOR, MERGER);

        verifyIteratorData(Arrays.asList(ENTRY1, ENTRY2, ENTRY3), iterator);
    }

}
