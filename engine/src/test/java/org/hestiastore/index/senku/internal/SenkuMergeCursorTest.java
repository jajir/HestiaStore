package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.Entry;
import org.hestiastore.index.EntryIteratorList;
import org.junit.jupiter.api.Test;

class SenkuMergeCursorTest {

    @Test
    void cursorAdvancesAndClosesExhaustedInput() {
        final EntryIteratorList<Integer, Integer> input =
                new EntryIteratorList<>(List.of(Entry.of(1, 10),
                        Entry.of(2, 20)));
        final SenkuMergeCursor<Integer, Integer> cursor =
                new SenkuMergeCursor<>(input, 3);

        assertEquals(3, cursor.ordinal());
        assertEquals(Entry.of(1, 10), cursor.current());
        cursor.advance();
        assertEquals(Entry.of(2, 20), cursor.current());
        cursor.advance();
        assertNull(cursor.current());
        assertTrue(input.wasClosed());
    }

    @Test
    void emptyInputIsClosedDuringConstruction() {
        final EntryIteratorList<Integer, Integer> input =
                new EntryIteratorList<>(List.of());

        final SenkuMergeCursor<Integer, Integer> cursor =
                new SenkuMergeCursor<>(input, 0);

        assertNull(cursor.current());
        assertTrue(input.wasClosed());
    }
}
