package com.coroptis.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

import com.coroptis.index.Pair;
import com.coroptis.index.PairIterator;
import com.coroptis.index.PairIteratorList;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;

public class MergeIteratorTest {
    final TypeDescriptorString tds = new TypeDescriptorString();
    final TypeDescriptorInteger tdi = new TypeDescriptorInteger();

    @Test
    void test_basic_merging() throws Exception {

        final List<Pair<Integer, String>> mainData = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"), Pair.of(6, "ddh"));

        final List<Pair<Integer, String>> cacheData = List.of(Pair.of(3, "ddi"),
                Pair.of(11, "ddm"));

        try (PairIterator<Integer, String> iterator = new MergeIterator<Integer, String>(
                new PairIteratorList<>(mainData),
                new PairIteratorList<>(cacheData), tdi, tds)) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(1, "bbb"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(2, "ccc"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(3, "ddi"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(6, "ddh"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(11, "ddm"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_main_is_empty() throws Exception {

        final List<Pair<Integer, String>> mainData = List.of();

        final List<Pair<Integer, String>> cacheData = List.of(Pair.of(3, "ddi"),
                Pair.of(11, "ddm"));

        try (PairIterator<Integer, String> iterator = new MergeIterator<Integer, String>(
                new PairIteratorList<>(mainData),
                new PairIteratorList<>(cacheData), tdi, tds)) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(3, "ddi"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(11, "ddm"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_cache_is_empty() throws Exception {

        final List<Pair<Integer, String>> mainData = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"), Pair.of(6, "ddh"));

        final List<Pair<Integer, String>> cacheData = List.of();

        try (PairIterator<Integer, String> iterator = new MergeIterator<Integer, String>(
                new PairIteratorList<>(mainData),
                new PairIteratorList<>(cacheData), tdi, tds)) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(1, "bbb"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(2, "ccc"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(6, "ddh"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_access_invalid_element() throws Exception {

        final List<Pair<Integer, String>> mainData = List.of(Pair.of(1, "bbb"));

        final List<Pair<Integer, String>> cacheData = List
                .of(Pair.of(3, "ddi"));

        try (PairIterator<Integer, String> iterator = new MergeIterator<Integer, String>(
                new PairIteratorList<>(mainData),
                new PairIteratorList<>(cacheData), tdi, tds)) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(1, "bbb"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(3, "ddi"), iterator.next());

            assertFalse(iterator.hasNext());

            assertThrows(NoSuchElementException.class, () -> {
                iterator.next();
            }, "There no next element.");
        }
    }

    @Test
    void test_update_element() throws Exception {

        final List<Pair<Integer, String>> mainData = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"));

        final List<Pair<Integer, String>> cacheData = List
                .of(Pair.of(2, "hhh"));

        try (PairIterator<Integer, String> iterator = new MergeIterator<Integer, String>(
                new PairIteratorList<>(mainData),
                new PairIteratorList<>(cacheData), tdi, tds)) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(1, "bbb"), iterator.next());

            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(2, "hhh"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_delete_end_element() throws Exception {

        final List<Pair<Integer, String>> mainData = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ccc"));

        final List<Pair<Integer, String>> cacheData = List
                .of(Pair.of(2, tds.getTombstone()));

        try (PairIterator<Integer, String> iterator = new MergeIterator<Integer, String>(
                new PairIteratorList<>(mainData),
                new PairIteratorList<>(cacheData), tdi, tds)) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(1, "bbb"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_delete_notExisting_element() throws Exception {

        final List<Pair<Integer, String>> mainData = List.of(Pair.of(1, "bbb"),
                Pair.of(4, "rrr"));

        final List<Pair<Integer, String>> cacheData = List
                .of(Pair.of(2, tds.getTombstone()));

        try (PairIterator<Integer, String> iterator = new MergeIterator<Integer, String>(
                new PairIteratorList<>(mainData),
                new PairIteratorList<>(cacheData), tdi, tds)) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(1, "bbb"), iterator.next());

            assertEquals(Pair.of(4, "rrr"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void test_delete_multiple_elements() throws Exception {

        final List<Pair<Integer, String>> mainData = List.of(Pair.of(1, "bbb"),
                Pair.of(2, "ooo"), Pair.of(4, "rrr"));

        final List<Pair<Integer, String>> cacheData = List.of(
                Pair.of(1, tds.getTombstone()), Pair.of(4, tds.getTombstone()),
                Pair.of(5, tds.getTombstone()));

        try (PairIterator<Integer, String> iterator = new MergeIterator<Integer, String>(
                new PairIteratorList<>(mainData),
                new PairIteratorList<>(cacheData), tdi, tds)) {
            assertTrue(iterator.hasNext());
            assertEquals(Pair.of(2, "ooo"), iterator.next());

            assertFalse(iterator.hasNext());
        }
    }

}
