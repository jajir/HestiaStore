package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
