package org.hestiastore.index.datablockfile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hestiastore.index.TestData;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;

class DataBlockTest {

    @Test
    void test_ofSequence_and_getBytesSequence() {
        final DataBlockPayload payload = TestData.PAYLOAD_1008;
        final DataBlockHeader header = DataBlockHeader.of(
                DataBlockHeader.MAGIC_NUMBER, payload.calculateCrc());
        final ByteSequence headerBytes = header.toBytesSequence();
        final ByteSequence payloadBytes = payload.getBytesSequence();
        final byte[] raw = new byte[headerBytes.length() + payloadBytes.length()];
        ByteSequences.copy(headerBytes, 0, raw, 0, headerBytes.length());
        ByteSequences.copy(payloadBytes, 0, raw, headerBytes.length(),
                payloadBytes.length());

        final DataBlock dataBlock = DataBlock.ofSequence(
                ByteSequences.wrap(raw), DataBlockPosition.of(0));

        assertEquals(DataBlockHeader.MAGIC_NUMBER,
                dataBlock.getHeader().getMagicNumber());
        assertEquals(payload.calculateCrc(), dataBlock.getHeader().getCrc());
        assertEquals(1024, dataBlock.getBytesSequence().length());
        assertEquals(payload.length(), dataBlock.getPayloadSequence().length());
        assertArrayEquals(payload.getBytesSequence().toByteArrayCopy(),
                dataBlock.getPayloadSequence().toByteArrayCopy());
        assertArrayEquals(raw, dataBlock.getBytesSequence().toByteArrayCopy());
    }
}
