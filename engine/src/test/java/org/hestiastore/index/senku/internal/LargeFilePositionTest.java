package org.hestiastore.index.senku.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class LargeFilePositionTest {

    @Test
    void of_roundTripsZeroAndManifestPartBoundary() {
        assertPosition(0L, 0);
        assertPosition(Integer.MAX_VALUE - 1L, Integer.MAX_VALUE);
    }

    @Test
    void of_supportsFullPackedPositionRange() {
        final LargeFilePosition position = LargeFilePosition.of(0xffff_ffffL,
                Integer.MAX_VALUE);

        assertEquals(Long.MAX_VALUE, position.getPacked());
        assertEquals(0xffff_ffffL, position.getPartNumber());
        assertEquals(Integer.MAX_VALUE, position.getLocalPosition());
    }

    @Test
    void fromPacked_reconstructsOpaqueToken() {
        final LargeFilePosition original = LargeFilePosition.of(42L, 1234);
        final LargeFilePosition restored = LargeFilePosition
                .fromPacked(original.getPacked());

        assertEquals(42L, restored.getPartNumber());
        assertEquals(1234, restored.getLocalPosition());
    }

    @Test
    void packedOrderFollowsPartThenLocalPosition() {
        final long first = LargeFilePosition.of(1L, Integer.MAX_VALUE)
                .getPacked();
        final long second = LargeFilePosition.of(2L, 0).getPacked();

        assertTrue(first < second);
    }

    @Test
    void of_rejectsInvalidComponents() {
        assertThrows(IllegalArgumentException.class,
                () -> LargeFilePosition.of(-1L, 0));
        assertThrows(IllegalArgumentException.class,
                () -> LargeFilePosition.of(0x1_0000_0000L, 0));
        assertThrows(IllegalArgumentException.class,
                () -> LargeFilePosition.of(0L, -1));
    }

    @Test
    void fromPacked_rejectsSignBit() {
        assertThrows(IllegalArgumentException.class,
                () -> LargeFilePosition.fromPacked(-1L));
    }

    private static void assertPosition(final long partNumber,
            final int localPosition) {
        final LargeFilePosition position = LargeFilePosition.of(partNumber,
                localPosition);
        assertEquals(partNumber, position.getPartNumber());
        assertEquals(localPosition, position.getLocalPosition());
        assertTrue(position.getPacked() >= 0L);
    }
}
