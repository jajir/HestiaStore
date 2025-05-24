package com.hestiastore.index.sst;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

import com.hestiastore.index.Pair;
import com.hestiastore.index.PairIterator;
import com.hestiastore.index.PairIteratorList;
import com.hestiastore.index.datatype.TypeDescriptorInteger;

public class LimitedPairIteratorTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    @Test
    void test_basic_usage() throws Exception {

        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"), Pair.of(3, "dde"), Pair.of(4, "ddf"),
                Pair.of(5, "ddg"), Pair.of(6, "ddh"), Pair.of(7, "ddi"),
                Pair.of(8, "ddj"), Pair.of(9, "ddk"), Pair.of(10, "ddl"),
                Pair.of(11, "ddm"));

        try (final PairIterator<Integer, String> iterator = new LimitedPairIterator<Integer, String>(
                new PairIteratorList<Integer, String>(data),
                tdi.getComparator(), 5, 7)) {

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(5, "ddg"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(6, "ddh"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(7, "ddi"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_start_at_first_element() throws Exception {

        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"), Pair.of(3, "dde"), Pair.of(4, "ddf"),
                Pair.of(5, "ddg"), Pair.of(6, "ddh"), Pair.of(7, "ddi"),
                Pair.of(8, "ddj"), Pair.of(9, "ddk"), Pair.of(10, "ddl"),
                Pair.of(11, "ddm"));

        try (final PairIterator<Integer, String> iterator = new LimitedPairIterator<Integer, String>(
                new PairIteratorList<Integer, String>(data),
                tdi.getComparator(), 1, 2)) {

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(1, "bbb"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(2, "ccc"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_end_at_last_element() throws Exception {

        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"), Pair.of(3, "dde"), Pair.of(4, "ddf"),
                Pair.of(5, "ddg"), Pair.of(6, "ddh"), Pair.of(7, "ddi"),
                Pair.of(8, "ddj"), Pair.of(9, "ddk"), Pair.of(10, "ddl"),
                Pair.of(11, "ddm"));

        try (final PairIterator<Integer, String> iterator = new LimitedPairIterator<Integer, String>(
                new PairIteratorList<Integer, String>(data),
                tdi.getComparator(), 10, 11)) {

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(10, "ddl"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(11, "ddm"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_start_before_first_element() throws Exception {

        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"), Pair.of(3, "dde"), Pair.of(4, "ddf"),
                Pair.of(5, "ddg"), Pair.of(6, "ddh"), Pair.of(7, "ddi"),
                Pair.of(8, "ddj"), Pair.of(9, "ddk"), Pair.of(10, "ddl"),
                Pair.of(11, "ddm"));

        try (final PairIterator<Integer, String> iterator = new LimitedPairIterator<Integer, String>(
                new PairIteratorList<Integer, String>(data),
                tdi.getComparator(), -101, 2)) {

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(1, "bbb"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(2, "ccc"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_end_after_last_element() throws Exception {

        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"), Pair.of(3, "dde"), Pair.of(4, "ddf"),
                Pair.of(5, "ddg"), Pair.of(6, "ddh"), Pair.of(7, "ddi"),
                Pair.of(8, "ddj"), Pair.of(9, "ddk"), Pair.of(10, "ddl"),
                Pair.of(11, "ddm"));

        try (final PairIterator<Integer, String> iterator = new LimitedPairIterator<Integer, String>(
                new PairIteratorList<Integer, String>(data),
                tdi.getComparator(), 10, 9867)) {

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(10, "ddl"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(11, "ddm"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_one_element() throws Exception {

        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"), Pair.of(3, "dde"), Pair.of(4, "ddf"),
                Pair.of(5, "ddg"), Pair.of(6, "ddh"), Pair.of(7, "ddi"),
                Pair.of(8, "ddj"), Pair.of(9, "ddk"), Pair.of(10, "ddl"),
                Pair.of(11, "ddm"));

        try (final PairIterator<Integer, String> iterator = new LimitedPairIterator<Integer, String>(
                new PairIteratorList<Integer, String>(data),
                tdi.getComparator(), 10, 10)) {

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(10, "ddl"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_no_data_elements() throws Exception {

        try (final PairIterator<Integer, String> limited = new LimitedPairIterator<Integer, String>(
                new PairIteratorList<Integer, String>(Collections.emptyList()),
                tdi.getComparator(), 10, 9867)) {

            assertFalse(limited.hasNext());
        }
    }

    @Test
    void test_no_data_match() throws Exception {

        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"), Pair.of(3, "dde"), Pair.of(4, "ddf"),
                Pair.of(5, "ddg"), Pair.of(6, "ddh"), Pair.of(7, "ddi"),
                Pair.of(8, "ddj"), Pair.of(9, "ddk"), Pair.of(10, "ddl"),
                Pair.of(11, "ddm"));

        try (final PairIterator<Integer, String> iterator = new LimitedPairIterator<Integer, String>(
                new PairIteratorList<Integer, String>(data),
                tdi.getComparator(), -110, -90)) {

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_no_such_element() throws Exception {

        final List<Pair<Integer, String>> data = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"), Pair.of(3, "dde"), Pair.of(4, "ddf"),
                Pair.of(5, "ddg"), Pair.of(6, "ddh"), Pair.of(7, "ddi"),
                Pair.of(8, "ddj"), Pair.of(9, "ddk"), Pair.of(10, "ddl"),
                Pair.of(11, "ddm"));

        try (final PairIterator<Integer, String> iterator = new LimitedPairIterator<Integer, String>(
                new PairIteratorList<Integer, String>(data),
                tdi.getComparator(), 10, 11)) {

            assertTrue(iterator.hasNext());
            iterator.next();
            iterator.next();
            assertThrows(NoSuchElementException.class, () -> {
                iterator.next();
            }, "There no next element.");
        }
    }

    @SuppressWarnings("resource")
    @Test
    void test_unordered_min_max() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> {
            new LimitedPairIterator<Integer, String>(
                    new PairIteratorList<Integer, String>(
                            Collections.emptyList()),
                    tdi.getComparator(), 10, 5);
        }, "Min key '10' have to be smalles than max key '5'.");
    }
}
