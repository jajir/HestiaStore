package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

class TypeDescriptorPrimitiveConvertorTest {

    @Test
    void testByteConvertor_inPlaceSerialization() {
        assertInPlaceConvertor(new TypeDescriptorByte().getTypeEncoder(),
                (byte) 42, 1);
    }

    @Test
    void testIntegerConvertor_inPlaceSerialization() {
        assertInPlaceConvertor(
                new TypeDescriptorInteger().getTypeEncoder(), 123_456_789,
                TypeDescriptorInteger.REQUIRED_BYTES);
    }

    @Test
    void testLongConvertor_inPlaceSerialization() {
        assertInPlaceConvertor(new TypeDescriptorLong().getTypeEncoder(),
                987_654_321_000L, TypeDescriptorLong.REQUIRED_BYTES);
    }

    @Test
    void testFloatConvertor_inPlaceSerialization() {
        assertInPlaceConvertor(new TypeDescriptorFloat().getTypeEncoder(),
                123.5f, 4);
    }

    @Test
    void testDoubleConvertor_inPlaceSerialization() {
        assertInPlaceConvertor(new TypeDescriptorDouble().getTypeEncoder(),
                123.5d, 8);
    }

    private <T> void assertInPlaceConvertor(final TypeEncoder<T> convertor,
            final T value, final int expectedLength) {
        final byte[] expected = TestEncoding.toByteArray(convertor, value);
        final EncodedBytes encoded = convertor.encode(value,
                new byte[expectedLength + 2]);
        assertEquals(expectedLength, encoded.getLength());
        assertArrayEquals(expected,
                Arrays.copyOf(encoded.getBytes(), encoded.getLength()));

        final EncodedBytes resized = convertor.encode(value,
                new byte[Math.max(0, expectedLength - 1)]);
        assertEquals(expectedLength, resized.getLength());
        assertArrayEquals(expected, Arrays.copyOf(resized.getBytes(),
                resized.getLength()));
    }
}
