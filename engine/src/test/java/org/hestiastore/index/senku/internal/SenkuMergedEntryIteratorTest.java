package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIterator;
import org.hestiastore.index.EntryIteratorList;
import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class SenkuMergedEntryIteratorTest {

    @Test
    void mergeHandlesZeroAndEmptyInputs() {
        final EntryIteratorList<Integer, Integer> empty = iterator();
        final SenkuMergedEntryIterator<Integer, Integer> merged = merge(
                List.of(empty));

        assertFalse(merged.hasNext());
        assertTrue(empty.wasClosed());
        assertThrows(NoSuchElementException.class, merged::next);
        merged.close();
    }

    @Test
    void mergeOrdersManySkewedInputs() {
        final SenkuMergedEntryIterator<Integer, Integer> merged = merge(List.of(
                iterator(entry(1, 10), entry(5, 50), entry(9, 90)),
                iterator(entry(2, 20)),
                iterator(entry(3, 30), entry(4, 40), entry(6, 60),
                        entry(7, 70), entry(8, 80))));

        assertEquals(List.of(entry(1, 10), entry(2, 20), entry(3, 30),
                entry(4, 40), entry(5, 50), entry(6, 60), entry(7, 70),
                entry(8, 80), entry(9, 90)), readAllAndClose(merged));
    }

    @Test
    void mergeCollapsesDuplicatesWithinAndAcrossInputs() {
        final SenkuMergedEntryIterator<Integer, Integer> merged = merge(List.of(
                iterator(entry(1, 1), entry(1, 2), entry(3, 3)),
                iterator(entry(1, 4), entry(2, 2)),
                iterator(entry(1, 8), entry(3, 7))));

        assertEquals(List.of(entry(1, 15), entry(2, 2), entry(3, 10)),
                readAllAndClose(merged));
    }

    @Test
    void mergeSupportsReverseComparatorWhenInputsMatchIt() {
        final SenkuMergedEntryIterator<Integer, Integer> merged =
                new SenkuMergedEntryIterator<>(
                        List.of(iterator(entry(4, 4), entry(2, 2)),
                                iterator(entry(3, 3), entry(1, 1))),
                        Comparator.reverseOrder(),
                        (key, first, second) -> first + second);

        assertEquals(List.of(entry(4, 4), entry(3, 3), entry(2, 2),
                entry(1, 1)), readAllAndClose(merged));
    }

    @Test
    void closeEarlyClosesEveryRemainingInput() {
        final EntryIteratorList<Integer, Integer> first = iterator(entry(1, 1),
                entry(3, 3));
        final EntryIteratorList<Integer, Integer> second = iterator(entry(2, 2),
                entry(4, 4));
        final SenkuMergedEntryIterator<Integer, Integer> merged = merge(
                List.of(first, second));
        assertEquals(entry(1, 1), merged.next());

        merged.close();

        assertTrue(first.wasClosed());
        assertTrue(second.wasClosed());
        assertFalse(merged.hasNext());
    }

    @Test
    void mergeFailureClosesEveryInputAndPreservesCause() {
        final EntryIteratorList<Integer, Integer> first = iterator(entry(1, 1));
        final EntryIteratorList<Integer, Integer> second = iterator(entry(1, 2));
        final IllegalStateException cause = new IllegalStateException("boom");
        final SenkuMergedEntryIterator<Integer, Integer> merged =
                new SenkuMergedEntryIterator<>(List.of(first, second),
                        Comparator.naturalOrder(),
                        (key, left, right) -> {
                            throw cause;
                        });

        final IndexException error = assertThrows(IndexException.class,
                merged::next);

        assertEquals(cause, error.getCause());
        assertTrue(first.wasClosed());
        assertTrue(second.wasClosed());
    }

    @Test
    void nullMergeResultFailsAndClosesInputs() {
        final EntryIteratorList<Integer, Integer> first = iterator(entry(1, 1));
        final EntryIteratorList<Integer, Integer> second = iterator(entry(1, 2));
        final SenkuMergedEntryIterator<Integer, Integer> merged =
                new SenkuMergedEntryIterator<>(List.of(first, second),
                        Comparator.naturalOrder(),
                        (key, left, right) -> null);

        assertThrows(IndexException.class, merged::next);
        assertTrue(first.wasClosed());
        assertTrue(second.wasClosed());
    }

    @Test
    void constructorFailureClosesAllInputs() {
        final EntryIteratorList<Integer, Integer> first = iterator(entry(1, 1));
        final EntryIteratorList<Integer, Integer> third = iterator(entry(3, 3));
        final List<EntryIterator<Integer, Integer>> inputs = Arrays.asList(first,
                null, third);

        assertThrows(IllegalArgumentException.class,
                () -> new SenkuMergedEntryIterator<>(inputs,
                        Comparator.naturalOrder(),
                        (key, left, right) -> left + right));
        assertTrue(first.wasClosed());
        assertTrue(third.wasClosed());
    }

    private static SenkuMergedEntryIterator<Integer, Integer> merge(
            final List<? extends EntryIterator<Integer, Integer>> inputs) {
        return new SenkuMergedEntryIterator<>(inputs, Comparator.naturalOrder(),
                (key, first, second) -> first + second);
    }

    @SafeVarargs
    private static EntryIteratorList<Integer, Integer> iterator(
            final Entry<Integer, Integer>... entries) {
        return new EntryIteratorList<>(List.of(entries));
    }

    private static Entry<Integer, Integer> entry(final int key,
            final int value) {
        return Entry.of(key, value);
    }

    private static List<Entry<Integer, Integer>> readAllAndClose(
            final SenkuMergedEntryIterator<Integer, Integer> merged) {
        final List<Entry<Integer, Integer>> entries = new ArrayList<>();
        while (merged.hasNext()) {
            entries.add(merged.next());
        }
        merged.close();
        return entries;
    }
}
