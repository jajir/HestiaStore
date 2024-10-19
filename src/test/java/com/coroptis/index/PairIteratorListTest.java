package com.coroptis.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

public class PairIteratorListTest {

    @Test
    void test_basic() throws Exception {
        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"), Pair.of(3, "dde"), Pair.of(11, "ddm"));

        try (PairIterator<Integer, String> iterator = new PairIteratorList<>(
                data)) {
            assertTrue(iterator.hasNext());
            assertTrue(iterator.readCurrent().isEmpty());
            assertEquals(Pair.of(1, "bbb"), iterator.next());
            assertEquals(Pair.of(1, "bbb"), iterator.readCurrent().get());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(2, "ccc"), iterator.next());
            assertEquals(Pair.of(2, "ccc"), iterator.readCurrent().get());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(3, "dde"), iterator.next());
            assertEquals(Pair.of(3, "dde"), iterator.readCurrent().get());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(11, "ddm"), iterator.next());
            assertEquals(Pair.of(11, "ddm"), iterator.readCurrent().get());

            assertFalse(iterator.hasNext());
            assertEquals(Pair.of(11, "ddm"), iterator.readCurrent().get());
        }
    }

    @Test
    void test_empty_list() throws Exception {
        try(final PairIterator<Integer, String> iterator = new PairIteratorList<>(
                Collections.emptyList())){

            assertFalse(iterator.hasNext());
            assertTrue(iterator.readCurrent().isEmpty());
        }
    }
}
