package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
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
        final byte[] expected = TestEncoding.toByteArray(convertor, value);

        final EncodedBytes encoded = convertor.encode(value,
                new byte[expected.length + 4]);
        assertEquals(expected.length, encoded.getLength());
        assertArrayEquals(expected, Arrays.copyOf(encoded.getBytes(),
                encoded.getLength()));

        final EncodedBytes resized = convertor.encode(value,
                new byte[expected.length - 1]);
        assertEquals(expected.length, resized.getLength());
        assertArrayEquals(expected, Arrays.copyOf(resized.getBytes(),
                resized.getLength()));
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
        final CompositeValue shorter = CompositeValue.of("A", 1.0D);
        final CompositeValue full = CompositeValue.of("A", 1.0D, 1);

        assertThrows(IndexException.class,
                () -> TDC.getComparator().compare(shorter, full));
    }

    @Test
    void test_comparator_typeMismatch_throws() {
        final CompositeValue mismatched = CompositeValue.of(1, 1.0D, 1);

        assertThrows(IndexException.class,
                () -> TDC.getComparator().compare(mismatched, mismatched));
    }

    @Test
    void test_comparator_nullValue_throws() {
        final CompositeValue invalid = CompositeValue.of((Object) null, 1.0D,
                1);
        final CompositeValue valid = CompositeValue.of("A", 1.0D, 1);

        assertThrows(IndexException.class,
                () -> TDC.getComparator().compare(invalid, valid));
    }

    @Test
    void test_constructor_copiesElementTypesList() {
        final List<TypeDescriptor<?>> types = new ArrayList<>(
                List.of(new TypeDescriptorShortString(),
                        new TypeDescriptorInteger()));
        final TypeDescriptorComposite descriptor = new TypeDescriptorComposite(
                types);
        types.clear();
        types.add(new TypeDescriptorLong());

        final CompositeValue value = CompositeValue.of("AA", 7);
        final byte[] bytes = TestEncoding.toByteArray(descriptor.getTypeEncoder(),
                value);
        final CompositeValue decoded = descriptor.getTypeDecoder().decode(bytes);

        assertEquals(value, decoded);
        assertEquals(2, descriptor.getTombstone().size());
    }

    private void testReadWrite(
            final TypeDescriptor<CompositeValue> typeDescriptor,
            final CompositeValue value) {

        final byte[] bytes = TestEncoding.toByteArray(
                typeDescriptor.getTypeEncoder(), value);

        final CompositeValue readValue = typeDescriptor.getTypeDecoder()
                .decode(bytes);

        assertEquals(value, readValue);
    }

}
