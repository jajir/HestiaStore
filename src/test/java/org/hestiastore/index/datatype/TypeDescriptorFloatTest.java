package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hestiastore.index.Bytes;
import org.junit.jupiter.api.Test;

class TypeDescriptorFloatTest {

    private static final TypeDescriptor<Float> TDF = new TypeDescriptorFloat();

    @Test
    void test_readWrite() {
        testReadWrite(TDF, 0.0f);
        testReadWrite(TDF, 1.0f);
        testReadWrite(TDF, -1.0f);
        testReadWrite(TDF, Float.MAX_VALUE);
        testReadWrite(TDF, Float.MIN_VALUE);
        testReadWrite(TDF, Float.NaN);
        testReadWrite(TDF, Float.POSITIVE_INFINITY);
        testReadWrite(TDF, Float.NEGATIVE_INFINITY);
    }

    private void testReadWrite(final TypeDescriptor<Float> typeDescriptor,
            final Float value) {

        final Bytes bytes = typeDescriptor.getConvertorToBytes()
                .toBytesBuffer(value);

        final Float readValue = typeDescriptor.getConvertorFromBytes()
                .fromBytes(bytes);

        assertEquals(value, readValue);
    }

}
