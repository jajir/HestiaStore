package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

class TypeDescriptorCompositeTest {

    private static final TypeDescriptor<String> TDS = new TypeDescriptorShortString();
    private static final TypeDescriptor<Double> TDF = new TypeDescriptorDouble();
    private static final TypeDescriptor<Integer> TDI = new TypeDescriptorInteger();
    private static final TypeDescriptor<CompositeValue> TDC = new TypeDescriptorComposite(
            List.of(TDS, TDF, TDI));

    @Test
    void test_readWrite() {
        testReadWrite(TDC, CompositeValue.of("Hello", 1.0D, 42));
        testReadWrite(TDC, TDC.getTombstone());
    }

    @Test
    void test_convertor_inPlaceSerialization() {
        final ConvertorToBytes<CompositeValue> convertor = TDC
                .getConvertorToBytes();
        final CompositeValue value = CompositeValue.of("Hello", 1.0D, 42);
        final byte[] expected = convertor.toBytes(value);

        assertEquals(expected.length, convertor.bytesLength(value));

        final byte[] destination = new byte[expected.length + 4];
        Arrays.fill(destination, (byte) 0x5A);
        final int written = convertor.toBytes(value, destination);
        assertEquals(expected.length, written);
        assertArrayEquals(expected, Arrays.copyOf(destination, written));

        assertThrows(IllegalArgumentException.class,
                () -> convertor.toBytes(value, new byte[expected.length - 1]));
    }

    private void testReadWrite(
            final TypeDescriptor<CompositeValue> typeDescriptor,
            final CompositeValue value) {

        final byte[] bytes = typeDescriptor.getConvertorToBytes()
                .toBytes(value);

        final CompositeValue readValue = typeDescriptor.getConvertorFromBytes()
                .fromBytes(bytes);

        assertEquals(value, readValue);
    }

}
