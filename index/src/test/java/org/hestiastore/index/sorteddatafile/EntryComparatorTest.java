package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;

import org.hestiastore.index.Entry;
import org.junit.jupiter.api.Test;

class EntryComparatorTest {

    @Test
    void test_constructor_missingKeyComparator() {
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> new EntryComparator<String, Integer>(null));

        assertEquals("Property 'keyComparator' must not be null.",
                err.getMessage());
    }

    @Test
    void test_compare_by_key() {
        final EntryComparator<String, Integer> comparator = new EntryComparator<>(
                Comparator.naturalOrder());

        assertTrue(comparator.compare(new Entry<>("a", 1),
                new Entry<>("b", 2)) < 0);
        assertTrue(comparator.compare(new Entry<>("b", 1),
                new Entry<>("a", 2)) > 0);
        assertEquals(0, comparator.compare(new Entry<>("a", 1),
                new Entry<>("a", 99)));
    }
}
