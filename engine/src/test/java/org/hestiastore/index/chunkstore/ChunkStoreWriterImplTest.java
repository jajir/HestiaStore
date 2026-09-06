package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.TestData;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ChunkStoreWriterImplTest {

    private static final int VERSION = 17;

    @Mock
    private CellStoreWriter cellStoreWriter;

    private ChunkStoreWriterImpl writer;

    @BeforeEach
    void setUp() {
        writer = new ChunkStoreWriterImpl(cellStoreWriter,
                List.of(new ChunkFilterDoNothing()));
    }

    @Test
    void writeSequenceDelegatesToCellStoreWriter() {
        final CellPosition expectedPosition = CellPosition
                .of(DataBlockSize.ofDataBlockSize(1024), 0);
        when(cellStoreWriter.writeSequence(any())).thenReturn(expectedPosition);

        final CellPosition actualPosition = writer.writeSequence(
                TestData.CHUNK_PAYLOAD_9.getBytesSequence(), VERSION);

        assertSame(expectedPosition, actualPosition);
        verify(cellStoreWriter).writeSequence(any());
    }

    @Test
    void closeClosesUnderlyingCellStoreWriter() {
        writer.close();

        assertTrue(writer.wasClosed());
        verify(cellStoreWriter).close();
    }

    @Test
    void preparedChunkPreservesMetadataAndDoesNotRunEncodingFilters() {
        writer = new ChunkStoreWriterImpl(cellStoreWriter, List.of(input -> {
            throw new AssertionError(
                    "Prepared chunks must not be encoded twice");
        }));
        final ByteSequence payload = TestData.CHUNK_PAYLOAD_9
                .getBytesSequence();
        final ChunkData chunk = ChunkData.ofSequence(42L, 123L,
                ChunkHeader.MAGIC_NUMBER, VERSION, payload);
        writer.writePreparedChunk(chunk);
        final ArgumentCaptor<ByteSequence> bytes = ArgumentCaptor
                .forClass(ByteSequence.class);
        verify(cellStoreWriter).writeSequence(bytes.capture());
        final ChunkHeader header = ChunkHeader
                .ofSequence(bytes.getValue().slice(0, ChunkHeader.HEADER_SIZE));
        assertEquals(42L, header.getFlags());
        assertEquals(123L, header.getCrc());
        assertEquals(VERSION, header.getVersion());
        assertArrayEquals(payload.toByteArray(),
                bytes.getValue()
                        .slice(ChunkHeader.HEADER_SIZE,
                                ChunkHeader.HEADER_SIZE + payload.length())
                        .toByteArray());
    }

    @Test
    void preparedChunkRejectsInvalidMagicNullAndClosedWriter() {
        final ChunkData chunk = ChunkData.ofSequence(0, 0,
                ChunkHeader.MAGIC_NUMBER, VERSION,
                TestData.CHUNK_PAYLOAD_9.getBytesSequence());
        final ChunkData invalid = chunk.withMagicNumber(0);
        assertThrows(IllegalArgumentException.class,
                () -> writer.writePreparedChunk(invalid));
        assertThrows(IllegalArgumentException.class,
                () -> writer.writePreparedChunk(null));
        writer.close();
        assertThrows(IllegalArgumentException.class,
                () -> writer.writePreparedChunk(chunk));
    }
}
