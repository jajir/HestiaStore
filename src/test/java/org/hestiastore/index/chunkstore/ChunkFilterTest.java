package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ChunkFilterTest {

    private static final Bytes PAYLOAD = Bytes.of(new byte[] { 1, 2, 3, 4, 5 });

    private ChunkData baseData;

    @BeforeEach
    void setUp() {
        baseData = ChunkData.of(0L, 0L, 0L, 1, PAYLOAD);
    }

    @Test
    void crc32WritingAndValidation() {
        final ChunkFilter writeFilter = new ChunkFilterCrc32Writing();
        final ChunkData withCrc = writeFilter.apply(baseData);
        final ChunkFilter validateFilter = new ChunkFilterCrc32Validation();
        validateFilter.apply(withCrc); // should not throw

        final ChunkData broken = withCrc
                .withPayload(Bytes.of(new byte[] { 42 }));
        assertThrows(IllegalStateException.class,
                () -> validateFilter.apply(broken));
    }

    @Test
    void snappyCompressAndDecompress() {
        final ChunkFilter compressor = new ChunkFilterSnappyCompress();
        final ChunkData compressed = compressor.apply(baseData);
        final ChunkFilter decompressor = new ChunkFilterSnappyDecompress();
        final ChunkData decompressed = decompressor.apply(compressed);
        assertEquals(PAYLOAD, decompressed.getPayload());
        // decompressing twice is invalid
        assertThrows(IllegalStateException.class,
                () -> decompressor.apply(baseData));
    }

    @Test
    void magicNumberWriteAndValidation() {
        final ChunkFilter writer = new ChunkFilterMagicNumberWriting();
        final ChunkData withMagic = writer.apply(baseData);
        assertEquals(ChunkHeader.MAGIC_NUMBER, withMagic.getMagicNumber());

        final ChunkFilter validator = new ChunkFilterMagicNumberValidation();
        validator.apply(withMagic); // should not throw

        final ChunkData invalid = baseData
                .withMagicNumber(ChunkHeader.MAGIC_NUMBER + 1);
        assertThrows(IllegalStateException.class,
                () -> validator.apply(invalid));
    }

    @Test
    void xorEncryptAndDecrypt() {
        final ChunkFilter encrypt = new ChunkFilterXorEncrypt();
        final ChunkData encrypted = encrypt.apply(baseData);
        final ChunkFilter decrypt = new ChunkFilterXorDecrypt();
        final ChunkData decrypted = decrypt.apply(encrypted);
        assertEquals(PAYLOAD, decrypted.getPayload());
        assertEquals(0L, decrypted.getFlags());
        assertThrows(IllegalStateException.class,
                () -> decrypt.apply(baseData));
    }
}
