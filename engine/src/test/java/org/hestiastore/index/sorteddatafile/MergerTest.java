package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.Entry;
import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class MergerTest {

    @Test
    void test_merge_missing_entry1() {
        final Merger<String, Integer> merger = (key, v1, v2) -> v1;
        final Entry<String, Integer> entry = new Entry<>("a", 1);
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> merger.merge(null, entry));

        assertEquals("Property 'entry1' must not be null.", err.getMessage());
    }

    @Test
    void test_merge_missing_entry2() {
        final Merger<String, Integer> merger = (key, v1, v2) -> v1;
        final Entry<String, Integer> entry = new Entry<>("a", 1);
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> merger.merge(entry, null));

        assertEquals("Property 'entry2' must not be null.", err.getMessage());
    }

    @Test
    void test_merge_with_different_keys() {
        final Merger<String, Integer> merger = (key, v1, v2) -> v1 + v2;
        final Entry<String, Integer> left = new Entry<>("a", 1);
        final Entry<String, Integer> right = new Entry<>("b", 2);
        final Exception err = assertThrows(IllegalArgumentException.class,
                () -> merger.merge(left, right));

        assertEquals("Comparing entry with different keys", err.getMessage());
    }

    @Test
    void test_merge_with_null_result() {
        final Merger<String, Integer> merger = (key, v1, v2) -> null;
        final Entry<String, Integer> entry1 = new Entry<>("a", 1);
        final Entry<String, Integer> entry2 = new Entry<>("a", 2);
        final String expectedMessage = String.format(
                "Results of merging values '%s' and '%s' cant't by null.",
                entry1, entry2);
        final Exception err = assertThrows(IndexException.class,
                () -> merger.merge(entry1, entry2));

        assertEquals(expectedMessage, err.getMessage());
    }

    @Test
    void test_merge_entries() {
        final Merger<String, Integer> merger = (key, v1, v2) -> v1 + v2;
        final Entry<String, Integer> merged = merger.merge(
                new Entry<>("a", 1), new Entry<>("a", 2));

        assertEquals(new Entry<>("a", 3), merged);
    }
}
