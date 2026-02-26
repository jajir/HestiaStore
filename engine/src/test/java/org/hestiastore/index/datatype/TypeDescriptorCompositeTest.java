package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.hestiastore.index.IndexException;
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
        final TypeEncoder<CompositeValue> convertor = TDC
                .getTypeEncoder();
        final CompositeValue value = CompositeValue.of("Hello", 1.0D, 42);
        final byte[] expected = TypeEncoder.toByteArray(convertor, value);

        assertEquals(expected.length, convertor.bytesLength(value));

        final byte[] destination = new byte[expected.length + 4];
        Arrays.fill(destination, (byte) 0x5A);
        final int written = convertor.toBytes(value, destination);
        assertEquals(expected.length, written);
        assertArrayEquals(expected, Arrays.copyOf(destination, written));

        assertThrows(IllegalArgumentException.class,
                () -> convertor.toBytes(value, new byte[expected.length - 1]));
    }

    @Test
    void test_comparator_lexicographic() {
        assertTrue(TDC.getComparator()
                .compare(CompositeValue.of("A", 1.0D, 1),
                        CompositeValue.of("B", 1.0D, 1)) < 0);
        assertTrue(TDC.getComparator()
                .compare(CompositeValue.of("A", 1.0D, 1),
                        CompositeValue.of("A", 2.0D, 1)) < 0);
        assertEquals(0,
                TDC.getComparator().compare(CompositeValue.of("A", 1.0D, 1),
                        CompositeValue.of("A", 1.0D, 1)));
    }

    @Test
    void test_comparator_sizeMismatch_throws() {
        assertThrows(IndexException.class,
                () -> TDC.getComparator().compare(CompositeValue.of("A", 1.0D),
                        CompositeValue.of("A", 1.0D, 1)));
    }

    @Test
    void test_comparator_typeMismatch_throws() {
        assertThrows(IndexException.class,
                () -> TDC.getComparator()
                        .compare(CompositeValue.of(1, 1.0D, 1),
                                CompositeValue.of(1, 1.0D, 1)));
    }

    @Test
    void test_comparator_nullValue_throws() {
        assertThrows(IndexException.class,
                () -> TDC.getComparator().compare(
                        CompositeValue.of((Object) null, 1.0D, 1),
                        CompositeValue.of("A", 1.0D, 1)));
    }

    private void testReadWrite(
            final TypeDescriptor<CompositeValue> typeDescriptor,
            final CompositeValue value) {

        final byte[] bytes = TypeEncoder.toByteArray(
                typeDescriptor.getTypeEncoder(), value);

        final CompositeValue readValue = typeDescriptor.getTypeDecoder()
                .decode(bytes);

        assertEquals(value, readValue);
    }

}
