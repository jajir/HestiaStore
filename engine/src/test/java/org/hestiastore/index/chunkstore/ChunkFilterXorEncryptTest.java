package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;

class ChunkFilterXorEncryptTest {

    private static final ByteSequence PAYLOAD = ByteSequences.wrap(
            new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF });

    @Test
    void apply_should_encrypt_payload_and_set_flag() {
        final ChunkData input = ChunkData.ofSequence(0L, 0L,
                ChunkHeader.MAGIC_NUMBER,
                1, PAYLOAD);
        final ChunkFilterXorEncrypt filter = new ChunkFilterXorEncrypt();

        final ChunkData result = filter.apply(input);

        final ByteSequence expected = ChunkFilterXorEncrypt.xorPayload(PAYLOAD);
        assertArrayEquals(expected.toByteArrayCopy(),
                result.getPayloadSequence().toByteArrayCopy());
        assertNotEquals(PAYLOAD, result.getPayloadSequence());
        assertEquals(input.getFlags() | ChunkFilterXorEncrypt.FLAG_ENCRYPTED,
                result.getFlags());
        assertEquals(input.getMagicNumber(), result.getMagicNumber());
        assertEquals(input.getCrc(), result.getCrc());
        assertEquals(input.getVersion(), result.getVersion());
    }
}
