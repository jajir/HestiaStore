package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;

class TypeDescriptorByteArrayTest {

    private static final TypeDescriptor<ByteSequence> TDBA = new TypeDescriptorByteArray();

    @Test
    void test_readWrite() {
        testReadWrite(TDBA, ByteSequences.copyOf(new byte[] {}));
        testReadWrite(TDBA,
                ByteSequences.copyOf(new byte[] { 0x0F, 0x00, 0x01 }));
        testReadWrite(TDBA, ByteSequences.copyOf("Ahoj lidi".getBytes()));
    }

    private void testReadWrite(final TypeDescriptor<ByteSequence> typeDescriptor,
            final ByteSequence value) {

        final ByteSequence bytes = typeDescriptor.getConvertorToBytes()
                .toBytesBuffer(value);

        final ByteSequence readValue = typeDescriptor
                .getConvertorFromBytes().fromBytes(bytes);

        assertEquals(value, readValue);
    }

}
