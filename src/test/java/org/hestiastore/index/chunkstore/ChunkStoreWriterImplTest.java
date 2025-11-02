package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequenceCrc32;
import org.hestiastore.index.bytes.Bytes;
import org.hestiastore.index.TestData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ChunkStoreWriterImplTest {

    private static final int VERSION = 7;

    private RecordingCellStoreWriter recorder;
    private ChunkStoreWriter writer;

    @BeforeEach
    void setUp() {
        recorder = new RecordingCellStoreWriter();
        writer = new ChunkStoreWriterImpl(recorder,
                List.of(new ChunkFilterMagicNumberWriting(),
                        new ChunkFilterCrc32Writing(),
                        new ChunkFilterDoNothing()));
    }

    @Test
    void test_writeProducesPaddedBytes() {
        final Bytes payloadBytes = Bytes.of(new byte[] { 1, 2, 3, 4, 5 });
        final ChunkPayload payload = ChunkPayload.of(payloadBytes);

        final CellPosition position = writer.write(payload, VERSION);

        assertEquals(recorder.position, position);

        final ByteSequence written = recorder.written;
        final int expectedLength = alignToCellSize(
                ChunkHeader.HEADER_SIZE + payload.length());
        assertEquals(expectedLength, written.length());

        final ChunkHeader header = ChunkHeaderCodec
                .decode(written.slice(0, ChunkHeader.HEADER_SIZE));
        assertEquals(ChunkHeader.MAGIC_NUMBER, header.getMagicNumber());
        assertEquals(VERSION, header.getVersion());
        assertEquals(payload.length(), header.getPayloadLength());
        assertEquals(expectedCrc(payloadBytes), header.getCrc());
        assertTrue((header.getFlags()
                & ChunkFilterMagicNumberWriting.FLAG_MASK) != 0);

        final ByteSequence payloadSlice = written.slice(ChunkHeader.HEADER_SIZE,
                ChunkHeader.HEADER_SIZE + payload.length());
        assertArrayEquals(payloadBytes.toByteArray(),
                payloadSlice.toByteArray());

        final int paddingLength = expectedLength
                - (ChunkHeader.HEADER_SIZE + payload.length());
        if (paddingLength > 0) {
            final ByteSequence paddingSlice = written
                    .slice(expectedLength - paddingLength, expectedLength);
            assertTrue(allZeros(paddingSlice));
        }
    }

    @Test
    void test_writeWithAlignedPayloadKeepsSize() {
        final byte[] data = new byte[CellPosition.CELL_SIZE * 2];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i + 1);
        }
        final ChunkPayload payload = ChunkPayload.of(Bytes.of(data));

        writer.write(payload, VERSION);

        final ByteSequence written = recorder.written;
        final int expectedLength = ChunkHeader.HEADER_SIZE + data.length;
        assertEquals(expectedLength, written.length());
        final ByteSequence paddingSlice = written.slice(expectedLength,
                written.length());
        assertEquals(0, paddingSlice.length());
    }

    private boolean allZeros(final ByteSequence sequence) {
        final byte[] data = sequence.toByteArray();
        for (final byte b : data) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }

    private long expectedCrc(final Bytes payload) {
        final ByteSequenceCrc32 crc = new ByteSequenceCrc32();
        crc.update(payload);
        return crc.getValue();
    }

    private int alignToCellSize(final int length) {
        final int cell = CellPosition.CELL_SIZE;
        final int remainder = length % cell;
        return remainder == 0 ? length : length + (cell - remainder);
    }

    private static final class RecordingCellStoreWriter
            implements CellStoreWriter {

        private final CellPosition position = CellPosition
                .of(TestData.DATA_BLOCK_SIZE, 0);
        private boolean closed;
        private ByteSequence written;

        @Override
        public CellPosition write(final ByteSequence bytes) {
            if (closed) {
                throw new IllegalStateException("Writer already closed");
            }
            written = bytes;
            return position;
        }

        @Override
        public boolean wasClosed() {
            return closed;
        }

        @Override
        public void close() {
            if (closed) {
                throw new IllegalStateException("Writer already closed");
            }
            closed = true;
        }
    }
}
