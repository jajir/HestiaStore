package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
        final TypeEncoder<ByteArray> convertor = TDBA.getTypeEncoder();
        final ByteArray value = ByteArray.of(new byte[] { 0x0F, 0x00, 0x01 });
        final byte[] expected = TestEncoding.toByteArray(convertor, value);

        final EncodedBytes encoded = convertor.encode(value,
                new byte[expected.length + 2]);
        assertEquals(expected.length, encoded.getLength());
        assertArrayEquals(expected, Arrays.copyOf(encoded.getBytes(),
                encoded.getLength()));

        final EncodedBytes resized = convertor.encode(value,
                new byte[expected.length - 1]);
        assertEquals(expected.length, resized.getLength());
        assertArrayEquals(expected, Arrays.copyOf(resized.getBytes(),
                resized.getLength()));
    }

    private void testReadWrite(final TypeDescriptor<ByteArray> typeDescriptor,
            final ByteArray value) {

        final byte[] bytes = TestEncoding.toByteArray(
                typeDescriptor.getTypeEncoder(), value);

        final ByteArray readValue = typeDescriptor.getTypeDecoder()
                .decode(bytes);

        assertEquals(value, readValue);
    }

}
