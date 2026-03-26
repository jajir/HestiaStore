package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;

class ChunkFilterAesGcmDecryptTest {

    private static final SecretKey KEY = new SecretKeySpec(
            new byte[] { 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2,
                    1 },
            "AES");
    private static final ByteSequence PAYLOAD = ByteSequences.wrap(
            new byte[] { 1, 3, 3, 7, 9, 11, 13 });

    @Test
    void applyDecryptsPayloadAndClearsFlag() {
        final ChunkData encrypted = new ChunkFilterAesGcmEncrypt(KEY)
                .apply(ChunkData.ofSequence(4L, 55L, ChunkHeader.MAGIC_NUMBER,
                        1, PAYLOAD));
        final ChunkFilterAesGcmDecrypt filter = new ChunkFilterAesGcmDecrypt(
                KEY);

        final ChunkData result = filter.apply(encrypted);

        assertArrayEquals(PAYLOAD.toByteArrayCopy(),
                result.getPayloadSequence().toByteArrayCopy());
        assertEquals(4L, result.getFlags());
        assertEquals(encrypted.getCrc(), result.getCrc());
        assertEquals(encrypted.getMagicNumber(), result.getMagicNumber());
        assertEquals(encrypted.getVersion(), result.getVersion());
    }

    @Test
    void applyThrowsWhenFlagIsNotSet() {
        final ChunkData input = ChunkData.ofSequence(0L, 0L,
                ChunkHeader.MAGIC_NUMBER, 1, PAYLOAD);
        final ChunkFilterAesGcmDecrypt filter = new ChunkFilterAesGcmDecrypt(
                KEY);

        final IllegalStateException exception = assertThrows(
                IllegalStateException.class, () -> filter.apply(input));

        assertEquals("Chunk payload is not marked as AES-GCM encrypted.",
                exception.getMessage());
    }

    @Test
    void applyThrowsWhenEncryptedPayloadIsTooShort() {
        final ChunkData input = ChunkData.ofSequence(
                ChunkFilterAesGcmEncrypt.FLAG_AES_GCM, 0L,
                ChunkHeader.MAGIC_NUMBER, 1,
                ByteSequences.wrap(new byte[16]));
        final ChunkFilterAesGcmDecrypt filter = new ChunkFilterAesGcmDecrypt(
                KEY);

        final IllegalStateException exception = assertThrows(
                IllegalStateException.class, () -> filter.apply(input));

        assertEquals("Encrypted payload is too short.",
                exception.getMessage());
    }

    @Test
    void applyThrowsWhenAuthenticatedHeaderChanges() {
        final ChunkData encrypted = new ChunkFilterAesGcmEncrypt(KEY)
                .apply(ChunkData.ofSequence(2L, 91L, ChunkHeader.MAGIC_NUMBER,
                        1, PAYLOAD))
                .withCrc(92L);
        final ChunkFilterAesGcmDecrypt filter = new ChunkFilterAesGcmDecrypt(
                KEY);

        final IndexException exception = assertThrows(IndexException.class,
                () -> filter.apply(encrypted));

        assertEquals("AES-GCM authentication failed for chunk payload",
                exception.getMessage());
        assertInstanceOf(javax.crypto.AEADBadTagException.class,
                exception.getCause());
    }
}
