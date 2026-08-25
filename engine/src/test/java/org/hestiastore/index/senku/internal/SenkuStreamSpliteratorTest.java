package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Spliterator;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.junit.jupiter.api.Test;

class SenkuStreamSpliteratorTest {

    @Test
    void advancesSequentiallyWithoutSplitting() {
        final SenkuStreamSpliterator<Integer, Long> spliterator =
                new SenkuStreamSpliterator<>(new EntryIteratorList<>(
                        List.of(Entry.of(1, 10L), Entry.of(2, 20L))),
                        Comparator.naturalOrder());
        final List<Entry<Integer, Long>> entries = new ArrayList<>();

        assertTrue(spliterator.tryAdvance(entries::add));
        assertTrue(spliterator.tryAdvance(entries::add));
        assertFalse(spliterator.tryAdvance(entries::add));

        assertEquals(List.of(Entry.of(1, 10L), Entry.of(2, 20L)), entries);
        assertNull(spliterator.trySplit());
        assertEquals(Long.MAX_VALUE, spliterator.estimateSize());
        assertTrue((spliterator.characteristics() & Spliterator.SORTED) != 0);
        assertEquals(0, spliterator.getComparator().compare(Entry.of(1, 1L),
                Entry.of(1, 2L)));
    }
}
