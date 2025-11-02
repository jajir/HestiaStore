package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.Bytes;
import org.hestiastore.index.bytes.ConcatenatedByteSequence;
import org.hestiastore.index.bytes.MutableBytes;
import org.hestiastore.index.TestData;
import org.junit.jupiter.api.Test;

public class ChunkTest {

    private static final int VERSION = 682;

    @Test
    void test_of_bytes() {
        final ChunkHeader chunkHeader = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, 9, TestData.CHUNK_PAYLOAD_9.calculateCrc());
        final ByteSequence headerSequence = chunkHeader.getBytes();
        final Bytes headerBytes = headerSequence instanceof Bytes
                ? (Bytes) headerSequence
                : Bytes.copyOf(headerSequence);
        final Bytes chunkBytes = Bytes.concat(headerBytes, TestData.BYTES_9);

        final Chunk chunk = Chunk.of(chunkBytes);

        assertNotNull(chunk);
        assertEquals(VERSION, chunk.getHeader().getVersion());
        assertEquals(ChunkHeader.MAGIC_NUMBER,
                chunk.getHeader().getMagicNumber());
    }

    @Test
    void ofBytes_acceptsNonBytesSequence() {
        final ChunkHeader header = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, TestData.BYTES_9.length(),
                TestData.CHUNK_PAYLOAD_9.calculateCrc());
        final MutableBytes serialized = MutableBytes
                .allocate(ChunkHeader.HEADER_SIZE + TestData.BYTES_9.length());
        serialized.setBytes(0, header.getBytes());
        serialized.setBytes(ChunkHeader.HEADER_SIZE, TestData.BYTES_9);

        final ByteSequence view = serialized.toByteSequence();
        final Chunk chunk = Chunk.of(view);

        assertEquals(VERSION, chunk.getHeader().getVersion());
        assertEquals(TestData.BYTES_9.length(), chunk.getPayload().length());
        assertSame(view, chunk.getBytes());
    }

    @Test
    void ofBytes_nullInputThrows() {
        assertThrows(IllegalArgumentException.class, () -> Chunk.of(null));
    }

    @Test
    void ofHeaderAndPayload_validatesDeclaredLength() {
        final ChunkHeader header = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, 1, TestData.CHUNK_PAYLOAD_9.calculateCrc());
        final ByteSequence payload = TestData.BYTES_9;

        assertThrows(IllegalArgumentException.class,
                () -> Chunk.of(header, payload));
    }

    @Test
    void ofHeaderAndPayload_acceptsViewPayload() {
        final ChunkHeader header = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, TestData.BYTES_9.length(),
                TestData.CHUNK_PAYLOAD_9.calculateCrc());
        final MutableBytes payload = MutableBytes
                .copyOf(TestData.BYTES_9.slice(0, TestData.BYTES_9.length()));

        final ByteSequence payloadView = payload.toByteSequence();
        final Chunk chunk = Chunk.of(header, payloadView);

        assertEquals(VERSION, chunk.getHeader().getVersion());
        assertEquals(TestData.BYTES_9.length(), chunk.getPayload().length());
        assertSame(ConcatenatedByteSequence.class,
                chunk.getBytes().getClass());
    }

    @Test
    void ofHeaderAndPayload_nullPayloadThrows() {
        final ChunkHeader header = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, TestData.BYTES_9.length(),
                TestData.CHUNK_PAYLOAD_9.calculateCrc());

        assertThrows(IllegalArgumentException.class,
                () -> Chunk.of(header, null));
    }

    @Test
    void ofBytes_headerTooSmallThrows() {
        final MutableBytes serialized = MutableBytes.allocate(ChunkHeader.HEADER_SIZE - 1);

        assertThrows(IllegalArgumentException.class,
                () -> Chunk.of(serialized.toByteSequence()));
    }

    @Test
    void ofHeaderAndPayload_returnsConcatenatedView() {
        final ChunkHeader header = ChunkHeader.of(ChunkHeader.MAGIC_NUMBER,
                VERSION, TestData.BYTES_9.length(),
                TestData.CHUNK_PAYLOAD_9.calculateCrc());
        final MutableBytes payloadBuffer = MutableBytes
                .copyOf(TestData.BYTES_9);
        final ByteSequence payloadView = payloadBuffer.toByteSequence();

        final Chunk chunk = Chunk.of(header, payloadView);

        assertEquals(ChunkHeader.HEADER_SIZE + payloadView.length(),
                chunk.getBytes().length());
        assertSame(ConcatenatedByteSequence.class,
                chunk.getBytes().getClass());

        payloadBuffer.setByte(0, (byte) 0x7F);
        assertEquals(0x7F & 0xFF,
                chunk.getBytes().getByte(ChunkHeader.HEADER_SIZE) & 0xFF);
    }

}
