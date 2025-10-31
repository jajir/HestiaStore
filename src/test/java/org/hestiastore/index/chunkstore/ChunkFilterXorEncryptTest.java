package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Bytes;
import org.junit.jupiter.api.Test;

class ChunkFilterXorEncryptTest {

    private static final Bytes PAYLOAD = Bytes.of(
            new byte[] { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF });

    @Test
    void apply_should_encrypt_payload_and_set_flag() {
        final ChunkData input = ChunkData.of(0L, 0L, ChunkHeader.MAGIC_NUMBER,
                1, PAYLOAD);
        final ChunkFilterXorEncrypt filter = new ChunkFilterXorEncrypt();

        final ChunkData result = filter.apply(input);

        final ByteSequence expectedSequence = ChunkFilterXorEncrypt
                .xorPayload(PAYLOAD);
        assertEquals(toBytes(expectedSequence), result.getPayload());
        assertNotEquals(PAYLOAD, result.getPayload());
        assertEquals(input.getFlags() | ChunkFilterXorEncrypt.FLAG_ENCRYPTED,
                result.getFlags());
        assertEquals(input.getMagicNumber(), result.getMagicNumber());
        assertEquals(input.getCrc(), result.getCrc());
        assertEquals(input.getVersion(), result.getVersion());
    }

    private static Bytes toBytes(final ByteSequence sequence) {
        return sequence instanceof Bytes ? (Bytes) sequence
                : Bytes.copyOf(sequence);
    }
}
