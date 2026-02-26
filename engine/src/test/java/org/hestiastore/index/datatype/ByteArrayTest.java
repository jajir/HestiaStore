package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ByteArrayTest {

    @Test
    void test_copyTo_withOffset() {
        final ByteArray value = ByteArray.of(new byte[] { 0x01, 0x02, 0x03 });
        final byte[] destination = new byte[] { 0x55, 0x55, 0x55, 0x55, 0x55 };

        final int written = value.copyTo(destination, 1);

        assertEquals(3, written);
        assertArrayEquals(new byte[] { 0x55, 0x01, 0x02, 0x03, 0x55 },
                destination);
    }

    @Test
    void test_copyTo_destinationTooSmall() {
        final ByteArray value = ByteArray.of(new byte[] { 0x01, 0x02, 0x03 });

        assertThrows(IllegalArgumentException.class,
                () -> value.copyTo(new byte[2]));
    }

    @Test
    void test_getBytes_returnsCopy() {
        final ByteArray value = ByteArray.of(new byte[] { 0x01 });
        final byte[] copy = value.getBytes();

        copy[0] = 0x02;

        assertArrayEquals(new byte[] { 0x01 }, value.getBytes());
    }
}
