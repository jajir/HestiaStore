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

public class PairIteratorListTest {

    @Test
    void test_basic() {
        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"), Pair.of(3, "dde"), Pair.of(11, "ddm"));

        try (PairIteratorWithCurrent<Integer, String> iterator = new PairIteratorList<>(
                data)) {
            assertTrue(iterator.getCurrent().isEmpty());
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(1, "bbb"), iterator.next());
            assertTrue(iterator.getCurrent().isPresent());
            assertEquals(Pair.of(1, "bbb"), iterator.getCurrent().get());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(2, "ccc"), iterator.next());
            assertTrue(iterator.getCurrent().isPresent());
            assertEquals(Pair.of(2, "ccc"), iterator.getCurrent().get());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(3, "dde"), iterator.next());
            assertTrue(iterator.getCurrent().isPresent());
            assertEquals(Pair.of(3, "dde"), iterator.getCurrent().get());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(11, "ddm"), iterator.next());
            assertTrue(iterator.getCurrent().isPresent());
            assertEquals(Pair.of(11, "ddm"), iterator.getCurrent().get());

            assertFalse(iterator.hasNext());
            assertTrue(iterator.getCurrent().isPresent());
            assertEquals(Pair.of(11, "ddm"), iterator.getCurrent().get());
        }
    }

    @Test
    void test_close() {
        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"), Pair.of(3, "dde"), Pair.of(11, "ddm"));

        try (PairIterator<Integer, String> iterator = new PairIteratorList<>(
                data)) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(1, "bbb"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(2, "ccc"), iterator.next());
            assertTrue(iterator.hasNext());

            iterator.close();
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_next_fails_when_there_is_no_data() {
        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"));
        try (PairIterator<Integer, String> iterator = new PairIteratorList<>(
                data)) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(1, "bbb"), iterator.next());

            assertFalse(iterator.hasNext());
            final Exception e = assertThrows(NoSuchElementException.class,
                    iterator::next);

            assertNull(e.getMessage());
        }
    }

    @Test
    void test_empty_list() {
        try (final PairIterator<Integer, String> iterator = new PairIteratorList<>(
                Collections.emptyList())) {

            assertFalse(iterator.hasNext());
        }
    }
}
