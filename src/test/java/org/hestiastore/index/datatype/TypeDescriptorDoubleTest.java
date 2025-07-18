package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TypeDescriptorDoubleTest {

    private static final TypeDescriptor<Double> TDF = new TypeDescriptorDouble();

    @Test
    void test_readWrite() {
        testReadWrite(TDF, 0.0D);
        testReadWrite(TDF, 1.0D);
        testReadWrite(TDF, -1.0D);
        testReadWrite(TDF, Double.MAX_VALUE);
        testReadWrite(TDF, Double.MIN_VALUE);
        testReadWrite(TDF, Double.NaN);
        testReadWrite(TDF, Double.POSITIVE_INFINITY);
        testReadWrite(TDF, Double.NEGATIVE_INFINITY);
    }

    private void testReadWrite(final TypeDescriptor<Double> typeDescriptor,
            final Double value) {

        final byte[] bytes = typeDescriptor.getConvertorToBytes()
                .toBytes(value);

        final Double readValue = typeDescriptor.getConvertorFromBytes()
                .fromBytes(bytes);

        assertEquals(value, readValue);
    }

}
