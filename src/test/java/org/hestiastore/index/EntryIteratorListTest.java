package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

class EntryIteratorListTest {

    @Test
    void test_basic() {
        final List<Entry<Integer, String>> data = List.of(Entry.of(1, "bbb"),
                Entry.of(2, "ccc"), Entry.of(3, "dde"), Entry.of(11, "ddm"));

        try (EntryIteratorWithCurrent<Integer, String> iterator = new EntryIteratorList<>(
                data)) {
            assertTrue(iterator.getCurrent().isEmpty());
            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(1, "bbb"), iterator.next());
            assertTrue(iterator.getCurrent().isPresent());
            assertEquals(Entry.of(1, "bbb"), iterator.getCurrent().get());

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(2, "ccc"), iterator.next());
            assertTrue(iterator.getCurrent().isPresent());
            assertEquals(Entry.of(2, "ccc"), iterator.getCurrent().get());

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(3, "dde"), iterator.next());
            assertTrue(iterator.getCurrent().isPresent());
            assertEquals(Entry.of(3, "dde"), iterator.getCurrent().get());

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(11, "ddm"), iterator.next());
            assertTrue(iterator.getCurrent().isPresent());
            assertEquals(Entry.of(11, "ddm"), iterator.getCurrent().get());

            assertFalse(iterator.hasNext());
            assertTrue(iterator.getCurrent().isPresent());
            assertEquals(Entry.of(11, "ddm"), iterator.getCurrent().get());
        }
    }

    @Test
    void test_close() {
        final List<Entry<Integer, String>> data = List.of(Entry.of(1, "bbb"),
                Entry.of(2, "ccc"), Entry.of(3, "dde"), Entry.of(11, "ddm"));

        try (EntryIterator<Integer, String> iterator = new EntryIteratorList<>(
                data)) {
            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(1, "bbb"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(2, "ccc"), iterator.next());
            assertTrue(iterator.hasNext());
        }
    }

    @Test
    void test_next_fails_when_there_is_no_data() {
        final List<Entry<Integer, String>> data = List.of(Entry.of(1, "bbb"));
        try (EntryIterator<Integer, String> iterator = new EntryIteratorList<>(
                data)) {
            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(1, "bbb"), iterator.next());

            assertFalse(iterator.hasNext());
            final Exception e = assertThrows(NoSuchElementException.class,
                    iterator::next);

            assertNull(e.getMessage());
        }
    }

    @Test
    void test_empty_list() {
        try (final EntryIterator<Integer, String> iterator = new EntryIteratorList<>(
                Collections.emptyList())) {

            assertFalse(iterator.hasNext());
        }
    }
}
