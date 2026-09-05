package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.bytes.ZeroByteSequence;
import org.junit.jupiter.api.Test;

class ChunkFilterZstdCompressTest {
    @Test
    void rejectsCompressedInputAndOversizedPageBeforeAllocating() {
        final var filter = new ChunkFilterZstdCompress(3);
        final var payload = ByteSequences.wrap(new byte[] { 7 });
        final var zstd = ChunkData.ofSequence(
                ChunkFilterZstdCompress.FLAG_COMPRESSED, 0,
                ChunkHeader.MAGIC_NUMBER, 3, payload);
        final var snappy = zstd
                .withFlags(1L << ChunkFilter.BIT_POSITION_SNAPPY_COMPRESSION);
        final var oversized = zstd.withFlags(0)
                .withPayloadSequence(new ZeroByteSequence(
                        ChunkFilterZstdCompress.MAX_PAYLOAD_BYTES + 1));
        assertThrows(IndexException.class, () -> filter.apply(zstd));
        assertThrows(IndexException.class, () -> filter.apply(snappy));
        assertThrows(IndexException.class, () -> filter.apply(oversized));
        assertThrows(IndexException.class,
                () -> new ChunkFilterZstdCompress(0));
    }

    @Test
    void compressesRepetitivePayloadAndPreservesHeaders() {
        final byte[] bytes = new byte[10000];
        final var input = ChunkData.ofSequence(5L, 7L, ChunkHeader.MAGIC_NUMBER,
                3, ByteSequences.wrap(bytes));
        final var encoded = new ChunkFilterZstdCompress(3).apply(input);
        assertTrue(encoded.getPayloadSequence().length() < 100);
        assertEquals(input.getFlags() | ChunkFilterZstdCompress.FLAG_COMPRESSED,
                encoded.getFlags());
        assertEquals(input.getVersion(), encoded.getVersion());
        assertEquals(input.getCrc(), encoded.getCrc());
        assertArrayEquals(bytes, new ChunkFilterZstdDecompress().apply(encoded)
                .getPayloadSequence().toByteArray());
    }

    @Test
    void keepsTinyIncompressiblePayloadRaw() {
        final var input = ChunkData.ofSequence(0, 0, ChunkHeader.MAGIC_NUMBER,
                3, ByteSequences.wrap(new byte[] { 7 }));
        final var encoded = new ChunkFilterZstdCompress(3).apply(input);
        assertEquals(0, encoded.getFlags());
        assertArrayEquals(new byte[] { 7 }, new ChunkFilterZstdDecompress()
                .apply(encoded).getPayloadSequence().toByteArray());
    }
}
