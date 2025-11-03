package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

class ByteSequencesTest {

    @Test
    void wrapReturnsViewWithoutCopy() {
        final byte[] data = { 1, 2, 3 };

        final ByteSequence view = ByteSequences.wrap(data);

        assertTrue(view instanceof ByteSequenceView);
        data[1] = 42;
        assertEquals(42, view.getByte(1));
    }

    @Test
    void wrapEmptyArrayReturnsEmptyBytes() {
        final ByteSequence wrapped = ByteSequences.wrap(new byte[0]);

        assertSame(ByteSequence.EMPTY, wrapped);
    }

    @Test
    void wrapNullArrayThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.wrap(null));
    }

    @Test
    void copyOfByteArrayCreatesDefensiveCopy() {
        final byte[] data = { 5, 6, 7 };

        final ByteSequence copy = ByteSequences.copyOf(data);

        assertTrue(copy instanceof ByteSequenceView);
        assertArrayEquals(data, copy.toByteArray());
        assertNotSame(data, ((ByteSequenceView) copy).toByteArray());
        data[0] = 99;
        assertEquals(5, copy.getByte(0));
    }

    @Test
    void copyOfEmptyArrayReturnsEmptyBytes() {
        final ByteSequence copy = ByteSequences.copyOf(new byte[0]);

        assertSame(ByteSequence.EMPTY, copy);
    }

    @Test
    void copyOfSequenceReturnsSameBytesInstance() {
        final ByteSequenceView bytes = ByteSequenceView
                .of(new byte[] { 9, 8, 7 });

        final ByteSequence copy = ByteSequences.copyOf(bytes);

        assertSame(bytes, copy);
    }

    @Test
    void copyOfSequenceFromMutableBytesSharesBackingArray() {
        final MutableBytes mutable = MutableBytes.wrap(new byte[] { 1, 2, 3 });

        final ByteSequence copy = ByteSequences.copyOf(mutable);

        assertSame(mutable, copy);
    }

    @Test
    void copyOfSequenceFromViewCreatesCopy() {
        final byte[] backing = { 10, 11, 12, 13 };
        final ByteSequence view = ByteSequences.viewOf(backing, 1, 3);

        final ByteSequence copy = ByteSequences.copyOf(view);

        assertTrue(copy instanceof ByteSequenceView);
        assertArrayEquals(new byte[] { 11, 12 }, copy.toByteArray());
        assertNotSame(backing, ((ByteSequenceView) copy).toByteArray());
    }

    @Test
    void copyOfSliceReturnsSameInstance() {
        final ByteSequence slice = ByteSequences.wrap(new byte[] { 4, 5, 6 })
                .slice(1, 3);

        final ByteSequence copy = ByteSequences.copyOf(slice);

        assertTrue(copy instanceof ByteSequenceView);
        assertArrayEquals(slice.toByteArray(), copy.toByteArray());
        assertNotSame(slice, copy);
    }

    @Test
    void padToLengthReturnsSameInstanceWhenAlreadyLongEnough() {
        final ByteSequence original = ByteSequenceView
                .of(new byte[] { 1, 2, 3 });

        final ByteSequence padded = ByteSequences.padToLength(original, 2);

        assertSame(original, padded);
    }

    @Test
    void padToLengthPadsWithZeros() {
        final ByteSequence original = ByteSequenceView
                .of(new byte[] { 1, 2, 3 });

        final ByteSequence padded = ByteSequences.padToLength(original, 5);

        assertTrue(padded instanceof ByteSequenceView);
        assertEquals(5, padded.length());
        assertArrayEquals(new byte[] { 1, 2, 3, 0, 0 }, padded.toByteArray());
    }

    @Test
    void padToLengthNegativeTargetThrows() {
        final ByteSequence original = ByteSequences.wrap(new byte[] { 1, 2 });

        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.padToLength(original, -1));
    }

    @Test
    void padToCellReturnsSameInstanceWhenAligned() {
        final ByteSequence original = ByteSequenceView
                .of(new byte[] { 1, 2, 3, 4 });

        final ByteSequence padded = ByteSequences.padToCell(original, 2);

        assertSame(original, padded);
    }

    @Test
    void padToCellPadsToNextMultiple() {
        final ByteSequence original = ByteSequenceView
                .of(new byte[] { 1, 2, 3 });

        final ByteSequence padded = ByteSequences.padToCell(original, 4);

        assertTrue(padded instanceof ByteSequenceView);
        assertEquals(4, padded.length());
        assertArrayEquals(new byte[] { 1, 2, 3, 0 }, padded.toByteArray());
    }

    @Test
    void padToCellZeroLengthReturnsSameInstance() {
        final ByteSequence padded = ByteSequences.padToCell(ByteSequence.EMPTY,
                4);

        assertSame(ByteSequence.EMPTY, padded);
    }

    @Test
    void padToCellWithNonPositiveCellSizeThrows() {
        final ByteSequence original = ByteSequences.wrap(new byte[] { 1, 2 });

        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.padToCell(original, 0));
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.padToCell(original, -4));
    }

    @Test
    void contentEqualsDetectsEquality() {
        final ByteSequence first = ByteSequences.wrap(new byte[] { 1, 2, 3 });
        final ByteSequence second = ByteSequences.wrap(new byte[] { 1, 2, 3 });

        assertTrue(ByteSequences.contentEquals(first, second));
    }

    @Test
    void contentEqualsDetectsDifference() {
        final ByteSequence first = ByteSequences.wrap(new byte[] { 1, 2, 3 });
        final ByteSequence second = ByteSequences.wrap(new byte[] { 1, 2, 4 });

        assertFalse(ByteSequences.contentEquals(first, second));
    }

    @Test
    void contentEqualsHandlesDifferentLengths() {
        final ByteSequence first = ByteSequences.wrap(new byte[] { 1, 2, 3 });
        final ByteSequence second = ByteSequenceView
                .of(new byte[] { 1, 2, 3, 4 });

        assertFalse(ByteSequences.contentEquals(first, second));
    }

    @Test
    void contentEqualsNullArgumentsThrow() {
        final ByteSequence sequence = ByteSequences.wrap(new byte[] { 1 });

        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.contentEquals(null, sequence));
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.contentEquals(sequence, null));
    }

    @Test
    void contentHashCodeMatchesArraysHashCode() {
        final byte[] data = { 1, -2, 3 };
        final ByteSequence sequence = ByteSequences.wrap(data);

        final int expected = Arrays.hashCode(data);
        final int hash = ByteSequences.contentHashCode(sequence);

        assertEquals(expected, hash);
    }

    @Test
    void contentHashCodeNullArgumentThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.contentHashCode(null));
    }

    @Test
    void copyCopiesRequestedRange() {
        final ByteSequence source = ByteSequences
                .wrap(new byte[] { 1, 2, 3, 4 });
        final byte[] target = new byte[] { 9, 9, 9, 9 };

        ByteSequences.copy(source, 1, target, 0, 2);

        assertArrayEquals(new byte[] { 2, 3, 9, 9 }, target);
    }

    @Test
    void copyZeroLengthLeavesTargetUntouched() {
        final ByteSequence source = ByteSequences.wrap(new byte[] { 7, 8, 9 });
        final byte[] target = new byte[] { 1, 2, 3 };

        ByteSequences.copy(source, 2, target, 1, 0);

        assertArrayEquals(new byte[] { 1, 2, 3 }, target);
    }

    @Test
    void copyNullSourceThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.copy(null, 0, new byte[1], 0, 1));
    }

    @Test
    void copySourceOffsetOutOfBoundsThrows() {
        final ByteSequence source = ByteSequences.wrap(new byte[] { 1, 2 });

        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.copy(source, 2, new byte[1], 0, 1));
    }

    @Test
    void copyTargetOffsetOutOfBoundsThrows() {
        final ByteSequence source = ByteSequences.wrap(new byte[] { 1, 2 });

        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.copy(source, 0, new byte[1], 1, 1));
    }

    @Test
    void copyFallbackHandlesNonCopyableSequence() {
        final ByteSequence sequence = new ByteSequence() {

            private final byte[] data = { 10, 11, 12 };

            @Override
            public int length() {
                return data.length;
            }

            @Override
            public byte getByte(final int index) {
                return data[index];
            }

            @Override
            public ByteSequence slice(final int fromInclusive,
                    final int toExclusive) {
                return ByteSequences.copyOf(Arrays.copyOfRange(data,
                        fromInclusive, toExclusive));
            }

            @Override
            public byte[] toByteArray() {
                return Arrays.copyOf(data, data.length);
            }
        };

        final byte[] target = new byte[3];
        ByteSequences.copy(sequence, 0, target, 0, 3);

        assertArrayEquals(new byte[] { 10, 11, 12 }, target);
    }
}
