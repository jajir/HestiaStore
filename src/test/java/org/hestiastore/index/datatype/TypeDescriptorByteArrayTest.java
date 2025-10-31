package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Bytes;
import org.junit.jupiter.api.Test;

class TypeDescriptorByteArrayTest {

    private static final TypeDescriptor<Bytes> TDBA = new TypeDescriptorByteArray();

    @Test
    void test_readWrite() {
        testReadWrite(TDBA, Bytes.of(new byte[] {}));
        testReadWrite(TDBA, Bytes.of(new byte[] { 0x0F, 0x00, 0x01 }));
        testReadWrite(TDBA, Bytes.of("Ahoj lidi".getBytes()));
    }

    private void testReadWrite(final TypeDescriptor<Bytes> typeDescriptor,
            final Bytes value) {

        final ByteSequence bytes = typeDescriptor.getConvertorToBytes()
                .toBytesBuffer(value);

        final Bytes readValue = typeDescriptor.getConvertorFromBytes()
                .fromBytes(bytes);

        assertEquals(value, readValue);
    }

}
