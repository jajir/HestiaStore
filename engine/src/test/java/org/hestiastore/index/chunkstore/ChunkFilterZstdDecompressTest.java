package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;

class ChunkFilterZstdDecompressTest {
    @Test
    void rejectsInvalidOrOversizedFramesBeforeAllocatingOutput() {
        final byte[][] invalid = { { 1, 2, 3 }, { 0x28, (byte) 0xb5, 0x2f,
                (byte) 0xfd, (byte) 0xa0, 1, 0, 0, 0x20 } };
        for (byte[] bytes : invalid) {
            final var chunk = ChunkData.ofSequence(
                    ChunkFilterZstdCompress.FLAG_COMPRESSED, 0,
                    ChunkHeader.MAGIC_NUMBER, 3, ByteSequences.wrap(bytes));
            final var reader = new ChunkFilterZstdDecompress();
            assertThrows(IndexException.class, () -> reader.apply(chunk));
        }
    }

    @Test
    void rejectsSnappyAndAcceptsExplicitRawPayload() {
        final var raw = ChunkData.ofSequence(0, 0, ChunkHeader.MAGIC_NUMBER, 3,
                ByteSequences.wrap(new byte[] { 1 }));
        final var snappy = raw
                .withFlags(1L << ChunkFilter.BIT_POSITION_SNAPPY_COMPRESSION);
        final var reader = new ChunkFilterZstdDecompress();
        assertSame(raw, reader.apply(raw));
        assertThrows(IndexException.class, () -> reader.apply(snappy));
    }
}
