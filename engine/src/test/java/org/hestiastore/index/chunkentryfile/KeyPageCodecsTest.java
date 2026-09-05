package org.hestiastore.index.chunkentryfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;

class KeyPageCodecsTest {
    @Test
    void stableIdsRoundTripAndUnknownOrOldIdsFail() {
        assertEquals(2, KeyPageCodecs.prefix().getId());
        assertEquals(3, KeyPageCodecs.longDeltaVarint().getId());
        assertFalse(KeyPageCodecs.fromId(2).isLongDeltaVarint());
        assertTrue(KeyPageCodecs.fromId(3).isLongDeltaVarint());
        assertThrows(IndexException.class, () -> KeyPageCodecs.fromId(1));
        assertThrows(IndexException.class, () -> KeyPageCodecs.fromId(99));
    }

    @Test
    void fixedWeightCodecRequiresCompleteDomainInsteadOfAnIdAlone() {
        final var codec = KeyPageCodecs.longFixedWeightDeltaVarint(4, 2,
                new long[] { 3 }, 0);
        assertEquals(4, codec.getId());
        assertTrue(codec.isLongDeltaVarint());
        assertTrue(codec.isLongFixedWeightDeltaVarint());
        assertThrows(IndexException.class, () -> KeyPageCodecs.fromId(4));
        assertThrows(IllegalArgumentException.class, () -> KeyPageCodecs
                .longFixedWeightDeltaVarint(4, 2, new long[] { 0 }, 1));
    }
}
