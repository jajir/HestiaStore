package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.Bytes;
import org.junit.jupiter.api.Test;

class ChunkFilterXorDecryptTest {

    private static final Bytes PAYLOAD = Bytes
            .of(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7 });

    @Test
    void apply_should_decrypt_payload_and_clear_flag() {
        final ByteSequence encryptedPayload = ChunkFilterXorEncrypt
                .xorPayload(PAYLOAD);
        final long flags = ChunkFilterXorEncrypt.FLAG_ENCRYPTED | 8L;
        final ChunkData input = ChunkData.of(flags, 0L,
                ChunkHeader.MAGIC_NUMBER, 1, encryptedPayload);
        final ChunkFilterXorDecrypt filter = new ChunkFilterXorDecrypt();

        final ChunkData result = filter.apply(input);

        assertEquals(PAYLOAD, result.getPayload());
        assertEquals(flags & ~ChunkFilterXorEncrypt.FLAG_ENCRYPTED,
                result.getFlags());
        assertEquals(input.getMagicNumber(), result.getMagicNumber());
        assertEquals(input.getCrc(), result.getCrc());
        assertEquals(input.getVersion(), result.getVersion());
    }

    @Test
    void apply_should_throw_when_flag_not_set() {
        final ChunkData input = ChunkData.of(0L, 0L, ChunkHeader.MAGIC_NUMBER,
                1, PAYLOAD);
        final ChunkFilterXorDecrypt filter = new ChunkFilterXorDecrypt();

        final IllegalStateException exception = assertThrows(
                IllegalStateException.class, () -> filter.apply(input));

        assertEquals("Chunk payload is not marked as encrypted.",
                exception.getMessage());
    }
}
