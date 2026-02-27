package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

class ChunkFilterSnappyDecompressTest {

    private static final ByteSequence PAYLOAD = ByteSequences
            .wrap(new byte[] { 10, 20, 30, 40, 50, 60, 70, 80 });

    @Test
    void apply_should_decompress_payload_and_clear_flag() throws IOException {
        final byte[] compressed = Snappy.compress(PAYLOAD.toByteArrayCopy());
        final ChunkData input = ChunkData.ofSequence(
                ChunkFilterSnappyCompress.FLAG_COMPRESSED, 0L,
                ChunkHeader.MAGIC_NUMBER, 1, ByteSequences.wrap(compressed));
        final ChunkFilterSnappyDecompress filter = new ChunkFilterSnappyDecompress();

        final ChunkData result = filter.apply(input);

        assertArrayEquals(PAYLOAD.toByteArrayCopy(),
                result.getPayloadSequence().toByteArrayCopy());
        assertEquals(
                input.getFlags() & ~ChunkFilterSnappyCompress.FLAG_COMPRESSED,
                result.getFlags());
        assertEquals(input.getMagicNumber(), result.getMagicNumber());
        assertEquals(input.getCrc(), result.getCrc());
        assertEquals(input.getVersion(), result.getVersion());
    }

    @Test
    void apply_should_throw_when_flag_not_set() {
        final ChunkData input = ChunkData.ofSequence(0L, 0L,
                ChunkHeader.MAGIC_NUMBER,
                1, PAYLOAD);
        final ChunkFilterSnappyDecompress filter = new ChunkFilterSnappyDecompress();

        assertThrows(IllegalStateException.class, () -> filter.apply(input));
    }

    @Test
    void apply_should_wrap_io_exception_into_index_exception() {
        final IOException ioException = new IOException("decompress-failed");
        final ChunkData input = ChunkData.ofSequence(1L, 0L,
                ChunkHeader.MAGIC_NUMBER,
                1, PAYLOAD);
        final ChunkFilterSnappyDecompress filter = new ChunkFilterSnappyDecompress() {
            @Override
            byte[] decompressPayload(final ByteSequence payload)
                    throws IOException {
                throw ioException;
            }
        };

        final Exception exception = assertThrows(IllegalStateException.class,
                () -> filter.apply(input));

        assertEquals("Chunk payload is not marked as Snappy compressed.",
                exception.getMessage());
    }
}
