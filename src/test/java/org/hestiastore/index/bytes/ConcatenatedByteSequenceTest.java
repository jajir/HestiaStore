package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ConcatenatedByteSequenceTest {

    @Test
    void test_getByte_reflectsConcatenation() {
        final ByteSequence first = ByteSequences.wrap(new byte[] { 1, 2, 3 });
        final ByteSequence second = ByteSequences.wrap(new byte[] { 4, 5 });

        final ByteSequence concatenated = ConcatenatedByteSequence.of(first,
                second);

        assertEquals(5, concatenated.length());
        assertEquals(1, concatenated.getByte(0));
        assertEquals(5, concatenated.getByte(4));
    }

    @Test
    void test_copyTo_acrossBoundary() {
        final ByteSequence first = ByteSequences.wrap(new byte[] { 1, 2, 3 });
        final ByteSequence second = ByteSequences.wrap(new byte[] { 4, 5, 6 });
        final ByteSequence concatenated = ConcatenatedByteSequence.of(first,
                second);

        final byte[] copy = new byte[4];
        concatenated.copyTo(2, copy, 0, copy.length);

        assertArrayEquals(new byte[] { 3, 4, 5, 6 }, copy);
    }

    @Test
    void test_slice_withinFirstDelegates() {
        final ByteSequence first = ByteSequenceView
                .of(new byte[] { 1, 2, 3, 4 });
        final ByteSequence second = ByteSequences.wrap(new byte[] { 5, 6 });
        final ByteSequence concatenated = ConcatenatedByteSequence.of(first,
                second);

        final ByteSequence slice = concatenated.slice(1, 3);

        assertArrayEquals(new byte[] { 2, 3 }, slice.toByteArray());
    }

    @Test
    void test_slice_withinSecondDelegates() {
        final ByteSequence first = ByteSequences.wrap(new byte[] { 1, 2, 3 });
        final ByteSequence second = ByteSequences.wrap(new byte[] { 4, 5, 6 });
        final ByteSequence concatenated = ConcatenatedByteSequence.of(first,
                second);

        final ByteSequence slice = concatenated.slice(3, 5);

        assertArrayEquals(new byte[] { 4, 5 }, slice.toByteArray());
    }

    @Test
    void test_slice_acrossBoundaryCreatesNestedView() {
        final ByteSequence first = ByteSequences.wrap(new byte[] { 1, 2, 3 });
        final ByteSequence second = ByteSequences.wrap(new byte[] { 4, 5, 6 });
        final ByteSequence concatenated = ConcatenatedByteSequence.of(first,
                second);

        final ByteSequence slice = concatenated.slice(2, 5);

        assertArrayEquals(new byte[] { 3, 4, 5 }, slice.toByteArray());
    }

    @Test
    void test_of_emptyInputsReturnOtherOperand() {
        final ByteSequence first = ByteSequence.EMPTY;
        final ByteSequence second = ByteSequences.wrap(new byte[] { 9 });

        assertSame(second, ConcatenatedByteSequence.of(first, second));
        assertSame(first,
                ConcatenatedByteSequence.of(first, ByteSequence.EMPTY));
    }

    @Test
    void test_of_nullOperandsThrow() {
        final ByteSequence valid = ByteSequences.wrap(new byte[] { 1 });

        assertThrows(IllegalArgumentException.class,
                () -> ConcatenatedByteSequence.of(null, valid));
        assertThrows(IllegalArgumentException.class,
                () -> ConcatenatedByteSequence.of(valid, null));
    }

    @Test
    void test_getByte_outOfBoundsThrows() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1 }),
                ByteSequences.wrap(new byte[] { 2 }));

        assertThrows(IllegalArgumentException.class,
                () -> concatenated.getByte(2));
    }

    @Test
    void test_getByte_negativeIndexThrows() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1 }),
                ByteSequences.wrap(new byte[] { 2 }));

        assertThrows(IllegalArgumentException.class,
                () -> concatenated.getByte(-1));
    }

    @Test
    void test_copyTo_zeroLengthNoOp() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2 }),
                ByteSequences.wrap(new byte[] { 3 }));
        final byte[] target = new byte[] { 9, 9, 9 };

        concatenated.copyTo(1, target, 0, 0);

        assertArrayEquals(new byte[] { 9, 9, 9 }, target);
    }

    @Test
    void test_slice_emptyRangeReturnsEmpty() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2 }),
                ByteSequences.wrap(new byte[] { 3 }));

        assertSame(ByteSequence.EMPTY, concatenated.slice(1, 1));
    }

    @Test
    void test_copyTo_nullTargetThrows() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1 }),
                ByteSequences.wrap(new byte[] { 2 }));

        assertThrows(IllegalArgumentException.class,
                () -> concatenated.copyTo(0, null, 0, 1));
    }

    @Test
    void test_copyTo_sourceRangeExceedsThrows() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2 }),
                ByteSequences.wrap(new byte[] { 3 }));
        final byte[] target = new byte[3];

        assertThrows(IllegalArgumentException.class,
                () -> concatenated.copyTo(2, target, 0, 2));
    }

    @Test
    void test_copyTo_targetRangeExceedsThrows() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2 }),
                ByteSequences.wrap(new byte[] { 3 }));
        final byte[] target = new byte[2];

        assertThrows(IllegalArgumentException.class,
                () -> concatenated.copyTo(0, target, 1, 2));
    }

    @Test
    void test_copyTo_negativeOffsetsThrow() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2 }),
                ByteSequences.wrap(new byte[] { 3 }));
        final byte[] target = new byte[2];

        assertThrows(IllegalArgumentException.class,
                () -> concatenated.copyTo(-1, target, 0, 1));
        assertThrows(IllegalArgumentException.class,
                () -> concatenated.copyTo(0, target, -1, 1));
    }

    @Test
    void test_slice_invalidRangesThrow() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2 }),
                ByteSequences.wrap(new byte[] { 3 }));

        assertThrows(IllegalArgumentException.class,
                () -> concatenated.slice(-1, 2));
        assertThrows(IllegalArgumentException.class,
                () -> concatenated.slice(0, 4));
        assertThrows(IllegalArgumentException.class,
                () -> concatenated.slice(2, 1));
    }

    @Test
    void test_length_overflowThrows() {
        final ByteSequence huge = new ByteSequence() {
            @Override
            public int length() {
                return Integer.MAX_VALUE;
            }

            @Override
            public byte getByte(final int index) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void copyTo(final int sourceOffset, final byte[] target,
                    final int targetOffset, final int length) {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte[] toByteArray() {
                final byte[] copy = new byte[length()];
                copyTo(0, copy, 0, copy.length);
                return copy;
            }

            @Override
            public ByteSequence slice(final int fromInclusive,
                    final int toExclusive) {
                throw new UnsupportedOperationException();
            }
        };

        assertThrows(ArithmeticException.class,
                () -> ConcatenatedByteSequence.of(huge, huge));
    }
}
