package org.hestiastore.index;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        final Bytes source = Bytes.of(new byte[] { 4, 5, 6 });
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
        final Bytes source = Bytes.of(new byte[] { 9, 8, 7 });

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
    void slice_returnsImmutableBytes() {
        final MutableBytes buffer = MutableBytes
                .wrap(new byte[] { 1, 2, 3, 4 });

        final ByteSequence slice = buffer.slice(1, 3);

        assertEquals(Bytes.of(new byte[] { 2, 3 }), Bytes.copyOf(slice));
    }

    @Test
    void toBytes_returnsIndependentCopy() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 5, 6, 7 });

        final Bytes bytesCopy = buffer.toBytes();
        buffer.setByte(0, (byte) 1);

        assertEquals(Bytes.of(new byte[] { 5, 6, 7 }), bytesCopy);
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
    void setBytes_outOfBoundsThrows() {
        final MutableBytes buffer = MutableBytes.allocate(2);
        final Bytes source = Bytes.of(new byte[] { 1, 2, 3 });

        assertThrows(IllegalArgumentException.class,
                () -> buffer.setBytes(0, source, 0, source.length()));
    }
}
