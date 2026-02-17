package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.EntryIteratorWithCurrent;
import org.junit.jupiter.api.Test;

class EntryIteratorWithCurrentComparatorTest {

    private static final Comparator<String> KEY_COMPARATOR = Comparator
            .naturalOrder();

    @Test
    void test_constructor_missingKeyComparator() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> new EntryIteratorWithCurrentComparator<String, Integer>(
                        null));

        assertEquals("Property 'keyComparator' must not be null.",
                err.getMessage());
    }

    @Test
    void test_compare_nullIterators() {
        final EntryIteratorWithCurrentComparator<String, Integer> comparator = new EntryIteratorWithCurrentComparator<>(
                KEY_COMPARATOR);

        assertEquals(0, comparator.compare(null, null));
        try (EntryIteratorWithCurrent<String, Integer> iterator = new EntryIteratorList<>(
                List.of(new Entry<>("a", 1)))) {
            assertEquals(-1, comparator.compare(null, iterator));
            assertEquals(1, comparator.compare(iterator, null));
        }
    }

    @Test
    void test_compare_with_current_entries() {
        final EntryIteratorWithCurrentComparator<String, Integer> comparator = new EntryIteratorWithCurrentComparator<>(
                KEY_COMPARATOR);

        try (EntryIteratorWithCurrent<String, Integer> iterator1 = new EntryIteratorList<>(
                List.of(new Entry<>("b", 1)));
                EntryIteratorWithCurrent<String, Integer> iterator2 = new EntryIteratorList<>(
                        List.of(new Entry<>("a", 2)))) {
            iterator1.next();
            iterator2.next();

            assertTrue(comparator.compare(iterator1, iterator2) > 0);
            assertTrue(comparator.compare(iterator2, iterator1) < 0);
        }
    }

    @Test
    void test_compare_when_current_missing() {
        final EntryIteratorWithCurrentComparator<String, Integer> comparator = new EntryIteratorWithCurrentComparator<>(
                KEY_COMPARATOR);

        try (EntryIteratorWithCurrent<String, Integer> iteratorWithCurrent = new EntryIteratorList<>(
                List.of(new Entry<>("a", 1)));
                EntryIteratorWithCurrent<String, Integer> iteratorNoCurrent = new EntryIteratorList<>(
                        List.of(new Entry<>("b", 2)))) {
            iteratorWithCurrent.next();

            assertEquals(1,
                    comparator.compare(iteratorWithCurrent, iteratorNoCurrent));
            assertEquals(-1,
                    comparator.compare(iteratorNoCurrent, iteratorWithCurrent));
            assertEquals(0,
                    comparator.compare(iteratorNoCurrent, iteratorNoCurrent));
        }
    }
}
