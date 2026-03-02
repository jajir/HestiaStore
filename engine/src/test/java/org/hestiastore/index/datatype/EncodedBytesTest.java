package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class EncodedBytesTest {

    @Test
    void constructorRejectsNullBytes() {
        assertThrows(IllegalArgumentException.class,
                () -> new EncodedBytes(null, 0));
    }

    @Test
    void constructorRejectsNegativeLength() {
        assertThrows(IllegalArgumentException.class,
                () -> new EncodedBytes(new byte[1], -1));
    }

    @Test
    void constructorRejectsLengthGreaterThanArraySize() {
        assertThrows(IllegalArgumentException.class,
                () -> new EncodedBytes(new byte[2], 3));
    }

    @Test
    void gettersReturnProvidedValues() {
        final byte[] bytes = new byte[] { 1, 2, 3 };
        final EncodedBytes encoded = new EncodedBytes(bytes, 2);

        assertArrayEquals(bytes, encoded.getBytes());
        assertEquals(2, encoded.getLength());
    }
}
