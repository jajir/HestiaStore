package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
                () -> ByteSequences.copy(EMPTY, 0, null, 0, 0));
    }

    @Test
    void copyTo_nonZeroSourceOffsetThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.copy(EMPTY, 1, new byte[0], 0, 0));
    }

    @Test
    void copyTo_nonZeroLengthThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.copy(EMPTY, 0, new byte[1], 0, 1));
    }

    @Test
    void copyTo_targetOffsetOutOfRangeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.copy(EMPTY, 0, new byte[1], 2, 0));
    }

    @Test
    void copyTo_validNoOpSucceeds() {
        final byte[] target = { 1, 2, 3 };
        ByteSequences.copy(EMPTY, 0, target, 0, 0);
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

    @Test
    void equalsOtherEmptySequenceReturnsTrue() {
        final ByteSequence otherEmpty = new ByteSequenceSlice(new byte[] { 1 },
                0, 0);

        assertTrue(EMPTY.equals(otherEmpty));
        assertTrue(otherEmpty.equals(EMPTY));
    }

    @Test
    void equalsNonEmptySequenceReturnsFalse() {
        final ByteSequence nonEmpty = ByteSequences.wrap(new byte[] { 1 });

        assertFalse(EMPTY.equals(nonEmpty));
        assertFalse(nonEmpty.equals(EMPTY));
    }

    @Test
    void equalsNonByteSequenceReturnsFalse() {
        assertFalse(EMPTY.equals(""));
    }

    @Test
    void hashCodeMatchesEmptyArrayHash() {
        assertEquals(1, EMPTY.hashCode());
    }
}
