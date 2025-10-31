package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

class ByteSequenceViewTest {

    @Test
    void viewReflectsChangesInBackingArray() {
        final byte[] data = new byte[] { 1, 2, 3, 4 };
        final ByteSequence view = ByteSequenceView.of(data, 1, 4);

        assertEquals(3, view.length());
        assertEquals(2, view.getByte(0));

        data[2] = 9;

        assertEquals(9, view.getByte(1));
    }

    @Test
    void nestedSliceAdjustsOffset() {
        final byte[] data = new byte[] { 10, 20, 30, 40, 50 };
        final ByteSequence view = ByteSequenceView.of(data, 1, 5);

        final ByteSequence nested = view.slice(1, 3);

        assertEquals(2, nested.length());
        final byte[] copy = new byte[2];
        nested.copyTo(0, copy, 0, copy.length);

        assertArrayEquals(new byte[] { 30, 40 }, copy);
    }

    @Test
    void zeroLengthSliceReturnsEmptyBytes() {
        final byte[] data = new byte[] { 7, 8, 9 };
        final ByteSequence view = ByteSequenceView.of(data, 0, 3);

        assertSame(Bytes.EMPTY, view.slice(3, 3));
    }
}
