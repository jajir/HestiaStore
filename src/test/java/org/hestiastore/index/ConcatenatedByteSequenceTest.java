package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ConcatenatedByteSequenceTest {

    @Test
    void test_getByte_reflectsConcatenation() {
        final ByteSequence first = Bytes.of(new byte[] { 1, 2, 3 });
        final ByteSequence second = Bytes.of(new byte[] { 4, 5 });

        final ByteSequence concatenated = ConcatenatedByteSequence.of(first,
                second);

        assertEquals(5, concatenated.length());
        assertEquals(1, concatenated.getByte(0));
        assertEquals(5, concatenated.getByte(4));
    }

    @Test
    void test_copyTo_acrossBoundary() {
        final ByteSequence first = Bytes.of(new byte[] { 1, 2, 3 });
        final ByteSequence second = Bytes.of(new byte[] { 4, 5, 6 });
        final ByteSequence concatenated = ConcatenatedByteSequence.of(first,
                second);

        final byte[] copy = new byte[4];
        concatenated.copyTo(2, copy, 0, copy.length);

        assertArrayEquals(new byte[] { 3, 4, 5, 6 }, copy);
    }

    @Test
    void test_slice_withinFirstDelegates() {
        final ByteSequence first = Bytes.of(new byte[] { 1, 2, 3, 4 });
        final ByteSequence second = Bytes.of(new byte[] { 5, 6 });
        final ByteSequence concatenated = ConcatenatedByteSequence.of(first,
                second);

        final ByteSequence slice = concatenated.slice(1, 3);

        assertArrayEquals(new byte[] { 2, 3 }, slice.toByteArray());
    }

    @Test
    void test_slice_withinSecondDelegates() {
        final ByteSequence first = Bytes.of(new byte[] { 1, 2, 3 });
        final ByteSequence second = Bytes.of(new byte[] { 4, 5, 6 });
        final ByteSequence concatenated = ConcatenatedByteSequence.of(first,
                second);

        final ByteSequence slice = concatenated.slice(3, 5);

        assertArrayEquals(new byte[] { 4, 5 }, slice.toByteArray());
    }

    @Test
    void test_slice_acrossBoundaryCreatesNestedView() {
        final ByteSequence first = Bytes.of(new byte[] { 1, 2, 3 });
        final ByteSequence second = Bytes.of(new byte[] { 4, 5, 6 });
        final ByteSequence concatenated = ConcatenatedByteSequence.of(first,
                second);

        final ByteSequence slice = concatenated.slice(2, 5);

        assertArrayEquals(new byte[] { 3, 4, 5 }, slice.toByteArray());
    }

    @Test
    void test_of_emptyInputsReturnOtherOperand() {
        final ByteSequence first = Bytes.EMPTY;
        final ByteSequence second = Bytes.of(new byte[] { 9 });

        assertSame(second, ConcatenatedByteSequence.of(first, second));
        assertSame(first, ConcatenatedByteSequence.of(first, Bytes.EMPTY));
    }

    @Test
    void test_of_nullOperandsThrow() {
        final ByteSequence valid = Bytes.of(new byte[] { 1 });

        assertThrows(IllegalArgumentException.class,
                () -> ConcatenatedByteSequence.of(null, valid));
        assertThrows(IllegalArgumentException.class,
                () -> ConcatenatedByteSequence.of(valid, null));
    }

    @Test
    void test_getByte_outOfBoundsThrows() {
        final ByteSequence concatenated = ConcatenatedByteSequence
                .of(Bytes.of(new byte[] { 1 }), Bytes.of(new byte[] { 2 }));

        assertThrows(IllegalArgumentException.class,
                () -> concatenated.getByte(2));
    }

    @Test
    void test_copyTo_zeroLengthNoOp() {
        final ByteSequence concatenated = ConcatenatedByteSequence
                .of(Bytes.of(new byte[] { 1, 2 }), Bytes.of(new byte[] { 3 }));
        final byte[] target = new byte[] { 9, 9, 9 };

        concatenated.copyTo(1, target, 0, 0);

        assertArrayEquals(new byte[] { 9, 9, 9 }, target);
    }

    @Test
    void test_slice_emptyRangeReturnsEmpty() {
        final ByteSequence concatenated = ConcatenatedByteSequence
                .of(Bytes.of(new byte[] { 1, 2 }), Bytes.of(new byte[] { 3 }));

        assertSame(Bytes.EMPTY, concatenated.slice(1, 1));
    }

    @Test
    void test_copyTo_nullTargetThrows() {
        final ByteSequence concatenated = ConcatenatedByteSequence
                .of(Bytes.of(new byte[] { 1 }), Bytes.of(new byte[] { 2 }));

        assertThrows(IllegalArgumentException.class,
                () -> concatenated.copyTo(0, null, 0, 1));
    }
}
