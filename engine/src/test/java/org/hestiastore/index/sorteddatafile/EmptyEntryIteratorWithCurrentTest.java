package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

class EmptyEntryIteratorWithCurrentTest {

    @Test
    void test_empty_iterator_behavior() {
        final EmptyEntryIteratorWithCurrent<String, Integer> iterator = new EmptyEntryIteratorWithCurrent<>();

        assertFalse(iterator.hasNext());
        assertTrue(iterator.getCurrent().isEmpty());
        assertThrows(NoSuchElementException.class, iterator::next);

        iterator.close();
    }
}
