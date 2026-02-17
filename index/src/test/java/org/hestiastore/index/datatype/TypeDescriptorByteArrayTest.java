package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TypeDescriptorByteArrayTest {

    private static final TypeDescriptor<ByteArray> TDBA = new TypeDescriptorByteArray();

    @Test
    void test_readWrite() {
        testReadWrite(TDBA, ByteArray.of(new byte[] {}));
        testReadWrite(TDBA, ByteArray.of(new byte[] { 0x0F, 0x00, 0x01 }));
        testReadWrite(TDBA, ByteArray.of("Ahoj lidi".getBytes()));
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
