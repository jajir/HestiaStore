package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.TestData;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
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
}
