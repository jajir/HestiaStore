package org.hestiastore.index.segmentindex;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.junit.jupiter.api.Test;

class LimitedEntryIteratorTest {

    private final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    @Test
    void test_basic_usage() {

        final List<Entry<Integer, String>> data = List.of(Entry.of(1, "bbb"),
                Entry.of(2, "ccc"), Entry.of(3, "dde"), Entry.of(4, "ddf"),
                Entry.of(5, "ddg"), Entry.of(6, "ddh"), Entry.of(7, "ddi"),
                Entry.of(8, "ddj"), Entry.of(9, "ddk"), Entry.of(10, "ddl"),
                Entry.of(11, "ddm"));

        try (final EntryIterator<Integer, String> iterator = new LimitedEntryIterator<Integer, String>(
                new EntryIteratorList<Integer, String>(data),
                tdi.getComparator(), 5, 7)) {

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(5, "ddg"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(6, "ddh"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(7, "ddi"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_start_at_first_element() {

        final List<Entry<Integer, String>> data = List.of(Entry.of(1, "bbb"),
                Entry.of(2, "ccc"), Entry.of(3, "dde"), Entry.of(4, "ddf"),
                Entry.of(5, "ddg"), Entry.of(6, "ddh"), Entry.of(7, "ddi"),
                Entry.of(8, "ddj"), Entry.of(9, "ddk"), Entry.of(10, "ddl"),
                Entry.of(11, "ddm"));

        try (final EntryIterator<Integer, String> iterator = new LimitedEntryIterator<Integer, String>(
                new EntryIteratorList<Integer, String>(data),
                tdi.getComparator(), 1, 2)) {

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(1, "bbb"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(2, "ccc"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_end_at_last_element() {

        final List<Entry<Integer, String>> data = List.of(Entry.of(1, "bbb"),
                Entry.of(2, "ccc"), Entry.of(3, "dde"), Entry.of(4, "ddf"),
                Entry.of(5, "ddg"), Entry.of(6, "ddh"), Entry.of(7, "ddi"),
                Entry.of(8, "ddj"), Entry.of(9, "ddk"), Entry.of(10, "ddl"),
                Entry.of(11, "ddm"));

        try (final EntryIterator<Integer, String> iterator = new LimitedEntryIterator<Integer, String>(
                new EntryIteratorList<Integer, String>(data),
                tdi.getComparator(), 10, 11)) {

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(10, "ddl"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(11, "ddm"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_start_before_first_element() {

        final List<Entry<Integer, String>> data = List.of(Entry.of(1, "bbb"),
                Entry.of(2, "ccc"), Entry.of(3, "dde"), Entry.of(4, "ddf"),
                Entry.of(5, "ddg"), Entry.of(6, "ddh"), Entry.of(7, "ddi"),
                Entry.of(8, "ddj"), Entry.of(9, "ddk"), Entry.of(10, "ddl"),
                Entry.of(11, "ddm"));

        try (final EntryIterator<Integer, String> iterator = new LimitedEntryIterator<Integer, String>(
                new EntryIteratorList<Integer, String>(data),
                tdi.getComparator(), -101, 2)) {

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(1, "bbb"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(2, "ccc"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_end_after_last_element() {

        final List<Entry<Integer, String>> data = List.of(Entry.of(1, "bbb"),
                Entry.of(2, "ccc"), Entry.of(3, "dde"), Entry.of(4, "ddf"),
                Entry.of(5, "ddg"), Entry.of(6, "ddh"), Entry.of(7, "ddi"),
                Entry.of(8, "ddj"), Entry.of(9, "ddk"), Entry.of(10, "ddl"),
                Entry.of(11, "ddm"));

        try (final EntryIterator<Integer, String> iterator = new LimitedEntryIterator<Integer, String>(
                new EntryIteratorList<Integer, String>(data),
                tdi.getComparator(), 10, 9867)) {

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(10, "ddl"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(11, "ddm"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_one_element() {

        final List<Entry<Integer, String>> data = List.of(Entry.of(1, "bbb"),
                Entry.of(2, "ccc"), Entry.of(3, "dde"), Entry.of(4, "ddf"),
                Entry.of(5, "ddg"), Entry.of(6, "ddh"), Entry.of(7, "ddi"),
                Entry.of(8, "ddj"), Entry.of(9, "ddk"), Entry.of(10, "ddl"),
                Entry.of(11, "ddm"));

        try (final EntryIterator<Integer, String> iterator = new LimitedEntryIterator<Integer, String>(
                new EntryIteratorList<Integer, String>(data),
                tdi.getComparator(), 10, 10)) {

            assertTrue(iterator.hasNext());
            assertEquals(Entry.of(10, "ddl"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_no_data_elements() {

        try (final EntryIterator<Integer, String> limited = new LimitedEntryIterator<Integer, String>(
                new EntryIteratorList<Integer, String>(Collections.emptyList()),
                tdi.getComparator(), 10, 9867)) {

            assertFalse(limited.hasNext());
        }
    }

    @Test
    void test_no_data_match() {

        final List<Entry<Integer, String>> data = List.of(Entry.of(1, "bbb"),
                Entry.of(2, "ccc"), Entry.of(3, "dde"), Entry.of(4, "ddf"),
                Entry.of(5, "ddg"), Entry.of(6, "ddh"), Entry.of(7, "ddi"),
                Entry.of(8, "ddj"), Entry.of(9, "ddk"), Entry.of(10, "ddl"),
                Entry.of(11, "ddm"));

        try (final EntryIterator<Integer, String> iterator = new LimitedEntryIterator<Integer, String>(
                new EntryIteratorList<Integer, String>(data),
                tdi.getComparator(), -110, -90)) {

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_no_such_element() {

        final List<Entry<Integer, String>> data = List.of(Entry.of(1, "bbb"),
                Entry.of(2, "ccc"), Entry.of(3, "dde"), Entry.of(4, "ddf"),
                Entry.of(5, "ddg"), Entry.of(6, "ddh"), Entry.of(7, "ddi"),
                Entry.of(8, "ddj"), Entry.of(9, "ddk"), Entry.of(10, "ddl"),
                Entry.of(11, "ddm"));

        try (final EntryIterator<Integer, String> iterator = new LimitedEntryIterator<Integer, String>(
                new EntryIteratorList<Integer, String>(data),
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
    void test_unordered_min_max() {
        final List<Entry<Integer, String>> list = Collections.emptyList();
        final Comparator<Integer> comparator = tdi.getComparator();
        final EntryIteratorList<Integer, String> entryIteratorList = new EntryIteratorList<>(
                list);
        assertThrows(IllegalArgumentException.class, () -> {
            new LimitedEntryIterator<Integer, String>(entryIteratorList,
                    comparator, 10, 5);
        }, "Min key '10' have to be smalles than max key '5'.");
    }
}
