package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ByteSequenceSliceTest {

    @Test
    void sliceFromView_exposesExpectedBytes() {
        final ByteSequenceView view = ByteSequenceView
                .of(new byte[] { 1, 2, 3, 4, 5 });

        final ByteSequence slice = view.slice(1, 4);

        assertTrue(slice instanceof ByteSequenceSlice);
        assertEquals(3, slice.length());
        assertEquals(2, slice.getByte(0));
        assertEquals(4, slice.getByte(2));
    }

    @Test
    void getByte_outOfBoundsThrows() {
        final ByteSequence slice = ByteSequenceView.of(new byte[] { 9, 8, 7 })
                .slice(0, 2);

        assertThrows(IllegalArgumentException.class, () -> slice.getByte(-1));
        assertThrows(IllegalArgumentException.class, () -> slice.getByte(2));
    }

    @Test
    void toByteArray_returnsDefensiveCopy() {
        final byte[] backing = { 5, 6, 7, 8 };
        final ByteSequence slice = ByteSequenceView.of(backing)
                .slice(1, 3).slice(0, 2);

        final byte[] copy = slice.toByteArray();

        assertArrayEquals(new byte[] { 6, 7 }, copy);
        backing[2] = 42;
        assertEquals(42, slice.getByte(1));
        copy[0] = 11;
        assertEquals(42, slice.getByte(1)); // ensure copy is independent
    }

    @Test
    void sliceOfSlice_returnsChainedView() {
        final ByteSequence slice = ByteSequenceView
                .of(new byte[] { 1, 2, 3, 4 }).slice(1, 4).slice(1, 2);

        assertTrue(slice instanceof ByteSequenceSlice);
        assertEquals(1, slice.length());
        assertEquals(3, slice.getByte(0));
    }

    @Test
    void slice_zeroLengthReturnsEmpty() {
        final ByteSequence baseSlice = ByteSequenceView
                .of(new byte[] { 7, 8, 9 }).slice(0, 2);

        assertSame(ByteSequence.EMPTY, baseSlice.slice(1, 1));
    }

    @Test
    void equalsAndHashCodeCompareContent() {
        final ByteSequence first = ByteSequenceView
                .of(new byte[] { 1, 2, 3, 4 }).slice(1, 4);
        final ByteSequence second = ByteSequenceView
                .of(new byte[] { 9, 2, 3, 4, 5 }).slice(1, 4);
        final ByteSequence different = ByteSequenceView
                .of(new byte[] { 1, 2, 3, 4 }).slice(0, 3);

        assertTrue(first.equals(second));
        assertEquals(first.hashCode(), second.hashCode());
        assertFalse(first.equals(different));
    }
}
