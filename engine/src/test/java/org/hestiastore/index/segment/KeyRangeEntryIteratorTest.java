package org.hestiastore.index.segment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KeyRangeEntryIteratorTest {

    private EntryIteratorList<String, Integer> delegate;
    private KeyRangeEntryIterator<String, Integer> iterator;

    @BeforeEach
    void setUp() {
        openIterator("b", "d");
    }

    @AfterEach
    void tearDown() {
        if (!iterator.wasClosed()) {
            iterator.close();
        }
    }

    @Test
    void returnsHalfOpenRange() {
        assertEquals(List.of(Entry.of("b", 20), Entry.of("c", 30)),
                readAll());
    }

    @Test
    void nullUpperBoundReturnsTail() {
        replaceIterator("c", null);

        assertEquals(List.of(Entry.of("c", 30), Entry.of("d", 40)),
                readAll());
    }

    @Test
    void equalBoundsReturnEmptyIterator() {
        replaceIterator("c", "c");

        assertFalse(iterator.hasNext());
    }

    @Test
    void nextThrowsAfterRangeIsExhausted() {
        readAll();

        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    void closeClosesDelegate() {
        iterator.close();

        assertTrue(delegate.wasClosed());
    }

    private void replaceIterator(final String fromInclusive,
            final String toExclusive) {
        iterator.close();
        openIterator(fromInclusive, toExclusive);
    }

    private void openIterator(final String fromInclusive,
            final String toExclusive) {
        delegate = new EntryIteratorList<>(List.of(Entry.of("a", 10),
                Entry.of("b", 20), Entry.of("c", 30),
                Entry.of("d", 40)));
        iterator = new KeyRangeEntryIterator<>(delegate,
                Comparator.naturalOrder(), fromInclusive, toExclusive);
    }

    private List<Entry<String, Integer>> readAll() {
        final List<Entry<String, Integer>> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);
        return result;
    }
}
