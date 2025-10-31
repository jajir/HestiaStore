package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

class ChunkFilterSnappyCompressTest {

    private static final Bytes PAYLOAD = Bytes.of(new byte[] {
            // intentionally repetitive to improve compression ratio
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 });

    @Test
    void apply_should_compress_payload_and_set_flag() throws IOException {
        final ChunkData input = ChunkData.of(0L, 0L, ChunkHeader.MAGIC_NUMBER,
                1, PAYLOAD);
        final ChunkFilterSnappyCompress filter = new ChunkFilterSnappyCompress();

        final ChunkData result = filter.apply(input);

        final byte[] uncompressed = Snappy
                .uncompress(result.getPayload().toByteArray());
        assertEquals(PAYLOAD, Bytes.of(uncompressed));
        assertTrue(
                (result.getFlags()
                        & ChunkFilterSnappyCompress.FLAG_COMPRESSED) != 0L,
                "Compression flag should be set");
        assertEquals(input.getMagicNumber(), result.getMagicNumber());
        assertEquals(input.getCrc(), result.getCrc());
        assertEquals(input.getVersion(), result.getVersion());
    }

    @Test
    void apply_should_wrap_io_exception_into_index_exception() {
        final ChunkData input = ChunkData.of(0L, 0L, ChunkHeader.MAGIC_NUMBER,
                1, PAYLOAD);
        final IOException ioException = new IOException("compress-failed");
        final ChunkFilterSnappyCompress filter = new ChunkFilterSnappyCompress() {
            @Override
            byte[] compressPayload(final ByteSequence payload)
                    throws IOException {
                throw ioException;
            }
        };

        final IndexException exception = assertThrows(IndexException.class,
                () -> filter.apply(input));

        assertEquals("Unable to compress chunk payload",
                exception.getMessage());
        assertEquals(ioException, exception.getCause());
    }
}
