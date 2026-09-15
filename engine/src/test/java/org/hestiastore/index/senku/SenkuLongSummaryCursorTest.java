package org.hestiastore.index.senku;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SenkuLongSummaryCursorTest {

    @Test
    void advancesKeysAndWeightsTogether() {
        final SenkuLongSummaryCursor cursor = new SenkuLongSummaryCursor(
                new long[] { -3, 7 }, new long[] { 8, 2 });
        assertEquals(-3, cursor.key());
        assertEquals(8, cursor.weight());
        assertTrue(cursor.advance());
        assertEquals(7, cursor.key());
        assertEquals(2, cursor.weight());
        assertFalse(cursor.advance());
    }
}
