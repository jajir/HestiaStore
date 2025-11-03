package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ByteSequenceEmptyTest {

    private static final ByteSequence EMPTY = ByteSequence.EMPTY;

    @Test
    void length_isZero() {
        assertEquals(0, EMPTY.length());
    }

    @Test
    void getByte_anyIndexThrows() {
        assertThrows(IllegalArgumentException.class, () -> EMPTY.getByte(0));
        assertThrows(IllegalArgumentException.class, () -> EMPTY.getByte(-1));
    }

    @Test
    void copyTo_nullTargetThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> EMPTY.copyTo(0, null, 0, 0));
    }

    @Test
    void copyTo_nonZeroSourceOffsetThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> EMPTY.copyTo(1, new byte[0], 0, 0));
    }

    @Test
    void copyTo_nonZeroLengthThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> EMPTY.copyTo(0, new byte[1], 0, 1));
    }

    @Test
    void copyTo_targetOffsetOutOfRangeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> EMPTY.copyTo(0, new byte[1], 2, 0));
    }

    @Test
    void copyTo_validNoOpSucceeds() {
        final byte[] target = { 1, 2, 3 };
        EMPTY.copyTo(0, target, 0, 0);
        assertEquals(1, target[0]);
        assertEquals(2, target[1]);
        assertEquals(3, target[2]);
    }

    @Test
    void slice_zeroRangeReturnsSameInstance() {
        assertSame(EMPTY, EMPTY.slice(0, 0));
    }

    @Test
    void slice_invalidRangeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> EMPTY.slice(1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> EMPTY.slice(-1, 0));
    }
}
