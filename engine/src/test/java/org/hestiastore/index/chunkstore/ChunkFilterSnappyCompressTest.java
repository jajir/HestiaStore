package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

class ChunkFilterSnappyCompressTest {

    private static final ByteSequence PAYLOAD = ByteSequences.wrap(new byte[] {
            // intentionally repetitive to improve compression ratio
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 });

    @Test
    void apply_should_compress_payload_and_set_flag() throws IOException {
        final ChunkData input = ChunkData.ofSequence(0L, 0L,
                ChunkHeader.MAGIC_NUMBER,
                1, PAYLOAD);
        final ChunkFilterSnappyCompress filter = new ChunkFilterSnappyCompress();

        final ChunkData result = filter.apply(input);

        final byte[] uncompressed = Snappy
                .uncompress(result.getPayloadSequence().toByteArrayCopy());
        assertArrayEquals(PAYLOAD.toByteArrayCopy(), uncompressed);
        assertNotEquals(0L,
                result.getFlags() & ChunkFilterSnappyCompress.FLAG_COMPRESSED,
                "Compression flag should be set");
        assertEquals(input.getMagicNumber(), result.getMagicNumber());
        assertEquals(input.getCrc(), result.getCrc());
        assertEquals(input.getVersion(), result.getVersion());
    }

    @Test
    void apply_should_wrap_io_exception_into_index_exception() {
        final ChunkData input = ChunkData.ofSequence(0L, 0L,
                ChunkHeader.MAGIC_NUMBER,
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
