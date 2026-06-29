package org.hestiastore.index.chunkstorecache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Comparator;
import java.util.List;

import org.hestiastore.index.Entry;
import org.junit.jupiter.api.Test;

class ParsedChunkPageTest {

    private static final Comparator<Integer> COMPARATOR =
            Integer::compareTo;

    @Test
    void find_returnsValueForHit() {
        final ParsedChunkPage<Integer, String> page = page();

        assertEquals("twenty", page.find(20, COMPARATOR));
    }

    @Test
    void find_returnsNullForMissInsideRange() {
        final ParsedChunkPage<Integer, String> page = page();

        assertNull(page.find(25, COMPARATOR));
    }

    @Test
    void find_returnsNullForKeyBeforeMin() {
        final ParsedChunkPage<Integer, String> page = page();

        assertNull(page.find(5, COMPARATOR));
    }

    @Test
    void find_returnsNullForKeyAfterMax() {
        final ParsedChunkPage<Integer, String> page = page();

        assertNull(page.find(35, COMPARATOR));
    }

    @Test
    void find_returnsBoundaryKeys() {
        final ParsedChunkPage<Integer, String> page = page();

        assertEquals("ten", page.find(10, COMPARATOR));
        assertEquals("thirty", page.find(30, COMPARATOR));
    }

    private static ParsedChunkPage<Integer, String> page() {
        return ParsedChunkPage.of(List.of(Entry.of(10, "ten"),
                Entry.of(20, "twenty"), Entry.of(30, "thirty")));
    }
}
