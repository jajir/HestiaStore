package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MutableBytesTest {

    @Test
    void allocate_initializesWithZeros() {
        final MutableBytes buffer = MutableBytes.allocate(4);

        assertEquals(4, buffer.length());
        assertArrayEquals(new byte[4], buffer.array());
    }

    @Test
    void wrap_exposesBackingArray() {
        final byte[] backing = new byte[] { 1, 2, 3 };
        final MutableBytes buffer = MutableBytes.wrap(backing);

        assertSame(backing, buffer.array());
    }

    @Test
    void copyOf_createsIndependentBuffer() {
        final ByteSequenceView source = ByteSequenceView
                .of(new byte[] { 4, 5, 6 });
        final MutableBytes buffer = MutableBytes.copyOf(source);

        assertArrayEquals(new byte[] { 4, 5, 6 }, buffer.array());
        buffer.setByte(0, (byte) 9);
        assertEquals(4, source.getByte(0));
    }

    @Test
    void setAndGetByte_updatesExpectedPosition() {
        final MutableBytes buffer = MutableBytes.allocate(2);

        buffer.setByte(0, (byte) 7);
        buffer.setByte(1, (byte) 8);

        assertEquals(7, buffer.getByte(0));
        assertEquals(8, buffer.getByte(1));
    }

    @Test
    void setBytes_copiesFromSourceSequence() {
        final MutableBytes buffer = MutableBytes.allocate(5);
        final ByteSequenceView source = ByteSequenceView
                .of(new byte[] { 9, 8, 7 });

        buffer.setBytes(1, source, 0, source.length());

        assertArrayEquals(new byte[] { 0, 9, 8, 7, 0 }, buffer.array());
    }

    @Test
    void copyTo_transfersRequestedRange() {
        final MutableBytes buffer = MutableBytes
                .wrap(new byte[] { 3, 4, 5, 6 });
        final byte[] target = new byte[2];

        buffer.copyTo(1, target, 0, 2);

        assertArrayEquals(new byte[] { 4, 5 }, target);
    }

    @Test
    void slice_reflectsBackingData() {
        final MutableBytes buffer = MutableBytes
                .wrap(new byte[] { 1, 2, 3, 4 });

        final ByteSequence slice = buffer.slice(1, 3);

        assertEquals(2, slice.length());
        assertEquals(2, slice.getByte(0));

        buffer.setByte(1, (byte) 9);

        assertEquals(9, slice.getByte(0));

        final byte[] copy = new byte[2];
        slice.copyTo(0, copy, 0, copy.length);
        assertArrayEquals(new byte[] { 9, 3 }, copy);
    }

    @Test
    void slice_zeroLengthReturnsEmptyBytes() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });

        final ByteSequence slice = buffer.slice(2, 2);

        assertSame(ByteSequence.EMPTY, slice);
    }

    @Test
    void toByteSequence_reflectsBackingArray() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });

        final ByteSequence view = buffer.toByteSequence();

        assertEquals(3, view.length());
        assertEquals(1, view.getByte(0));

        buffer.setByte(0, (byte) 9);

        assertEquals(9, view.getByte(0));
    }

    @Test
    void toByteSequence_zeroLengthReturnsEmpty() {
        final MutableBytes buffer = MutableBytes.allocate(0);

        assertSame(ByteSequence.EMPTY, buffer.toByteSequence());
    }

    @Test
    void toImmutableBytes_sharesBackingArray() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 4, 5 });

        final ByteSequence view = buffer.toImmutableBytes();

        assertTrue(view instanceof ByteSequenceView);
        final ByteSequenceView bytesView = (ByteSequenceView) view;
        assertEquals(2, bytesView.length());
        assertEquals(4, bytesView.getByte(0));

        buffer.setByte(0, (byte) 9);

        assertEquals(9, bytesView.getByte(0));
    }

    @Test
    void toImmutableBytes_zeroLengthReturnsEmpty() {
        final MutableBytes buffer = MutableBytes.allocate(0);

        assertSame(ByteSequence.EMPTY, buffer.toImmutableBytes());
    }

    @Test
    void allocate_negativeSizeThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> MutableBytes.allocate(-1));
    }

    @Test
    void getByte_outOfBoundsThrows() {
        final MutableBytes buffer = MutableBytes.allocate(1);

        assertThrows(IllegalArgumentException.class, () -> buffer.getByte(1));
    }

    @Test
    void getByte_negativeIndexThrows() {
        final MutableBytes buffer = MutableBytes.allocate(1);

        assertThrows(IllegalArgumentException.class, () -> buffer.getByte(-1));
    }

    @Test
    void setBytes_outOfBoundsThrows() {
        final MutableBytes buffer = MutableBytes.allocate(2);
        final ByteSequenceView source = ByteSequenceView
                .of(new byte[] { 1, 2, 3 });

        assertThrows(IllegalArgumentException.class,
                () -> buffer.setBytes(0, source, 0, source.length()));
    }

    @Test
    void wrap_nullArrayThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> MutableBytes.wrap((byte[]) null));
    }

    @Test
    void copyOf_nullSequenceThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> MutableBytes.copyOf(null));
    }

    @Test
    void copyTo_zeroLengthDoesNothing() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });
        final byte[] target = new byte[] { 9, 9, 9 };

        buffer.copyTo(3, target, 1, 0);

        assertArrayEquals(new byte[] { 9, 9, 9 }, target);
    }

    @Test
    void copyTo_invalidSourceRangeThrows() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });
        final byte[] target = new byte[2];

        assertThrows(IllegalArgumentException.class,
                () -> buffer.copyTo(2, target, 0, 3));
    }

    @Test
    void copyTo_invalidTargetRangeThrows() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });
        final byte[] target = new byte[1];

        assertThrows(IllegalArgumentException.class,
                () -> buffer.copyTo(0, target, 0, 2));
    }

    @Test
    void copyTo_negativeSourceOffsetThrows() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });
        final byte[] target = new byte[1];

        assertThrows(IllegalArgumentException.class,
                () -> buffer.copyTo(-1, target, 0, 1));
    }

    @Test
    void copyTo_negativeTargetOffsetThrows() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });
        final byte[] target = new byte[1];

        assertThrows(IllegalArgumentException.class,
                () -> buffer.copyTo(0, target, -1, 1));
    }

    @Test
    void copyTo_negativeLengthThrows() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });
        final byte[] target = new byte[1];

        assertThrows(IllegalArgumentException.class,
                () -> buffer.copyTo(0, target, 0, -1));
    }

    @Test
    void setByte_negativeIndexThrows() {
        final MutableBytes buffer = MutableBytes.allocate(1);

        assertThrows(IllegalArgumentException.class,
                () -> buffer.setByte(-1, (byte) 1));
    }

    @Test
    void setByte_indexBeyondLengthThrows() {
        final MutableBytes buffer = MutableBytes.allocate(1);

        assertThrows(IllegalArgumentException.class,
                () -> buffer.setByte(1, (byte) 1));
    }

    @Test
    void setBytes_zeroLengthAtCapacityAllowed() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });
        final ByteSequenceView source = (ByteSequenceView) ByteSequences
                .wrap(new byte[] { 7, 8, 9 });

        buffer.setBytes(3, source, 0, 0);

        assertArrayEquals(new byte[] { 1, 2, 3 }, buffer.array());
    }

    @Test
    void setBytes_negativeTargetOffsetThrows() {
        final MutableBytes buffer = MutableBytes.allocate(2);
        final ByteSequenceView source = (ByteSequenceView) ByteSequences
                .wrap(new byte[] { 1 });

        assertThrows(IllegalArgumentException.class,
                () -> buffer.setBytes(-1, source, 0, 1));
    }

    @Test
    void setBytes_nullSourceThrows() {
        final MutableBytes buffer = MutableBytes.allocate(1);

        assertThrows(IllegalArgumentException.class,
                () -> buffer.setBytes(0, null, 0, 1));
    }

    @Test
    void setBytes_negativeLengthThrows() {
        final MutableBytes buffer = MutableBytes.allocate(1);
        final ByteSequenceView source = (ByteSequenceView) ByteSequences
                .wrap(new byte[] { 1 });

        assertThrows(IllegalArgumentException.class,
                () -> buffer.setBytes(0, source, 0, -1));
    }

    @Test
    void setBytes_sourceRangeOutOfBoundsThrows() {
        final MutableBytes buffer = MutableBytes.allocate(2);
        final ByteSequenceView source = (ByteSequenceView) ByteSequences
                .wrap(new byte[] { 1, 2 });

        assertThrows(IllegalArgumentException.class,
                () -> buffer.setBytes(0, source, 2, 1));
    }

    @Test
    void slice_negativeFromThrows() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });

        assertThrows(IllegalArgumentException.class, () -> buffer.slice(-1, 2));
    }

    @Test
    void slice_toBeyondLengthThrows() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });

        assertThrows(IllegalArgumentException.class, () -> buffer.slice(0, 4));
    }

    @Test
    void slice_fromGreaterThanToThrows() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });

        assertThrows(IllegalArgumentException.class, () -> buffer.slice(2, 1));
    }
}
