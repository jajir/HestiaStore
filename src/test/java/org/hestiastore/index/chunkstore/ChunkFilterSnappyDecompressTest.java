package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

class ChunkFilterSnappyDecompressTest {

    private static final Bytes PAYLOAD = Bytes
            .of(new byte[] { 10, 20, 30, 40, 50, 60, 70, 80 });

    @Test
    void apply_should_decompress_payload_and_clear_flag() throws IOException {
        final byte[] compressed = Snappy.compress(PAYLOAD.getData());
        final ChunkData input = ChunkData.of(
                ChunkFilterSnappyCompress.FLAG_COMPRESSED, 0L,
                ChunkHeader.MAGIC_NUMBER, 1, Bytes.of(compressed));
        final ChunkFilterSnappyDecompress filter = new ChunkFilterSnappyDecompress();

        final ChunkData result = filter.apply(input);

        assertEquals(PAYLOAD, result.getPayload());
        assertEquals(
                input.getFlags() & ~ChunkFilterSnappyCompress.FLAG_COMPRESSED,
                result.getFlags());
        assertEquals(input.getMagicNumber(), result.getMagicNumber());
        assertEquals(input.getCrc(), result.getCrc());
        assertEquals(input.getVersion(), result.getVersion());
    }

    @Test
    void apply_should_throw_when_flag_not_set() {
        final ChunkData input = ChunkData.of(0L, 0L, ChunkHeader.MAGIC_NUMBER,
                1, PAYLOAD);
        final ChunkFilterSnappyDecompress filter = new ChunkFilterSnappyDecompress();

        assertThrows(IllegalStateException.class, () -> filter.apply(input));
    }

    @Test
    void apply_should_wrap_io_exception_into_index_exception() {
        final IOException ioException = new IOException("decompress-failed");
        final ChunkData input = ChunkData.of(1L, 0L, ChunkHeader.MAGIC_NUMBER,
                1, PAYLOAD);
        final ChunkFilterSnappyDecompress filter = new ChunkFilterSnappyDecompress() {
            @Override
            byte[] decompressPayload(final Bytes payload) throws IOException {
                throw ioException;
            }
        };

        final Exception exception = assertThrows(IllegalStateException.class,
                () -> filter.apply(input));

        assertEquals("Chunk payload is not marked as Snappy compressed.",
                exception.getMessage());
    }
}
