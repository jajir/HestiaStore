package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

class TypeDescriptorByteArrayTest {

    private static final TypeDescriptor<ByteArray> TDBA = new TypeDescriptorByteArray();

    @Test
    void test_readWrite() {
        testReadWrite(TDBA, ByteArray.of(new byte[] {}));
        testReadWrite(TDBA, ByteArray.of(new byte[] { 0x0F, 0x00, 0x01 }));
        testReadWrite(TDBA, ByteArray.of("Ahoj lidi".getBytes()));
    }

    @Test
    void test_convertor_inPlaceSerialization() {
        final ConvertorToBytes<ByteArray> convertor = TDBA.getConvertorToBytes();
        final ByteArray value = ByteArray.of(new byte[] { 0x0F, 0x00, 0x01 });
        final byte[] expected = convertor.toBytes(value);

        assertEquals(expected.length, convertor.bytesLength(value));

        final byte[] destination = new byte[expected.length + 2];
        Arrays.fill(destination, (byte) 0x5A);
        final int written = convertor.toBytes(value, destination);

        assertEquals(expected.length, written);
        assertArrayEquals(expected, Arrays.copyOf(destination, written));
        assertThrows(IllegalArgumentException.class,
                () -> convertor.toBytes(value, new byte[expected.length - 1]));
    }

    private void testReadWrite(final TypeDescriptor<ByteArray> typeDescriptor,
            final ByteArray value) {

        final byte[] bytes = typeDescriptor.getConvertorToBytes()
                .toBytes(value);

        final ByteArray readValue = typeDescriptor.getConvertorFromBytes()
                .fromBytes(bytes);

        assertEquals(value, readValue);
    }

}
