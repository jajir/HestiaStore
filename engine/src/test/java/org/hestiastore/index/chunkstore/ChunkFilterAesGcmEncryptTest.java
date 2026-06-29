package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;

class ChunkFilterAesGcmEncryptTest {

    private static final SecretKey KEY = new SecretKeySpec(
            new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                    16 },
            "AES");
    private static final ByteSequence PAYLOAD = ByteSequences.wrap(
            new byte[] { 11, 22, 33, 44, 55, 66 });

    @Test
    void applyEncryptsPayloadAndSetsFlag() {
        final ChunkData input = ChunkData.ofSequence(8L, 123L,
                ChunkHeader.MAGIC_NUMBER, 2, PAYLOAD);
        final ChunkFilterAesGcmEncrypt filter = new ChunkFilterAesGcmEncrypt(
                KEY);

        final ChunkData result = filter.apply(input);

        assertNotEquals(PAYLOAD, result.getPayloadSequence());
        assertEquals(PAYLOAD.length() + 12 + (128 / Byte.SIZE),
                result.getPayloadSequence().length());
        assertEquals(input.getFlags() | ChunkFilterAesGcmEncrypt.FLAG_AES_GCM,
                result.getFlags());
        assertEquals(input.getCrc(), result.getCrc());
        assertEquals(input.getMagicNumber(), result.getMagicNumber());
        assertEquals(input.getVersion(), result.getVersion());

        final ChunkFilterAesGcmDecrypt decryptFilter = new ChunkFilterAesGcmDecrypt(
                KEY);
        final ChunkData decrypted = decryptFilter.apply(result);
        assertArrayEquals(PAYLOAD.toByteArrayCopy(),
                decrypted.getPayloadSequence().toByteArrayCopy());
    }
}
