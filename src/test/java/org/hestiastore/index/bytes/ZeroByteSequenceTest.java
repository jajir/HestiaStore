package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ZeroByteSequenceTest {

    @Test
    void length_returnsConfiguredSize() {
        final ZeroByteSequence zeros = new ZeroByteSequence(5);

        assertEquals(5, zeros.length());
    }

    @Test
    void getByte_returnsZeroWithinBounds() {
        final ZeroByteSequence zeros = new ZeroByteSequence(3);

        assertEquals(0, zeros.getByte(0));
        assertEquals(0, zeros.getByte(2));
    }

    @Test
    void getByte_outOfBoundsThrows() {
        final ZeroByteSequence zeros = new ZeroByteSequence(2);

        assertThrows(IllegalArgumentException.class, () -> zeros.getByte(-1));
        assertThrows(IllegalArgumentException.class, () -> zeros.getByte(2));
    }

    @Test
    void copyTo_fillsTargetRangeWithZeros() {
        final ZeroByteSequence zeros = new ZeroByteSequence(4);
        final byte[] target = new byte[] { 1, 1, 1, 1, 1 };

        ByteSequences.copy(zeros, 1, target, 2, 2);

        assertArrayEquals(new byte[] { 1, 1, 0, 0, 1 }, target);
    }

    @Test
    void copyTo_zeroLengthDoesNothing() {
        final ZeroByteSequence zeros = new ZeroByteSequence(4);
        final byte[] target = new byte[] { 2, 2, 2 };

        ByteSequences.copy(zeros, 2, target, 1, 0);

        assertArrayEquals(new byte[] { 2, 2, 2 }, target);
    }

    @Test
    void copyTo_invalidRangesThrow() {
        final ZeroByteSequence zeros = new ZeroByteSequence(3);
        final byte[] target = new byte[3];

        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.copy(zeros, -1, target, 0, 1));
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.copy(zeros, 0, target, -1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.copy(zeros, 1, target, 0, 3));
    }

    @Test
    void toByteArray_createsNewZeroFilledArray() {
        final ZeroByteSequence zeros = new ZeroByteSequence(3);

        final byte[] copy = zeros.toByteArray();

        assertArrayEquals(new byte[] { 0, 0, 0 }, copy);
        copy[0] = 42;
        assertEquals(0, zeros.getByte(0));
    }

    @Test
    void toByteArray_zeroLengthReturnsEmptyArray() {
        final ZeroByteSequence zeros = new ZeroByteSequence(0);

        assertEquals(0, zeros.toByteArray().length);
    }

    @Test
    void slice_returnsEmptyWhenNoBytesRequested() {
        final ZeroByteSequence zeros = new ZeroByteSequence(4);

        assertSame(ByteSequence.EMPTY, zeros.slice(2, 2));
    }

    @Test
    void slice_returnsSequenceOfRequestedLength() {
        final ZeroByteSequence zeros = new ZeroByteSequence(6);

        final ByteSequence slice = zeros.slice(1, 5);

        assertEquals(4, slice.length());
        assertEquals(0, slice.getByte(0));
        assertEquals(0, slice.getByte(3));
    }

    @Test
    void slice_invalidRangesThrow() {
        final ZeroByteSequence zeros = new ZeroByteSequence(3);

        assertThrows(IllegalArgumentException.class,
                () -> zeros.slice(-1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> zeros.slice(2, 1));
        assertThrows(IllegalArgumentException.class,
                () -> zeros.slice(0, 4));
    }
}
