package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.TestData;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CellStoreWriterImplTest {

    private static final CellPosition CELL_POSITION_0 = CellPosition
            .of(TestData.DATA_BLOCK_SIZE, 0);
    private static final ByteSequence BYTES_64 = TestData.BYTES_1024.slice(0,
            64);
    private static final ByteSequence BYTES_16 = TestData.BYTES_1024.slice(0,
            16);

    @Mock
    private CellStoreWriterCursor cursor;

    private CellStoreWriter writer;

    @Test
    void test_write_64_bytes() {
        when(cursor.getAvailableBytes()).thenReturn(64);
        when(cursor.getNextCellPosition()).thenReturn(CELL_POSITION_0);
        assertEquals(0, writer.writeSequence(BYTES_64).getValue());
        writer.close();

        final ArgumentCaptor<ByteSequence> sequenceCaptor = ArgumentCaptor
                .forClass(ByteSequence.class);
        verify(cursor).writeSequence(sequenceCaptor.capture());
        assertEquals(true, ByteSequences.contentEquals(BYTES_64,
                sequenceCaptor.getValue()));

        // verify that cursor write wasn't called more than once
        verify(cursor, times(1)).writeSequence(any(ByteSequence.class));
    }

    @Test
    void test_write_16_bytes() {
        when(cursor.getAvailableBytes()).thenReturn(64);
        when(cursor.getNextCellPosition()).thenReturn(CELL_POSITION_0);
        assertEquals(0, writer.writeSequence(BYTES_16).getValue());
        writer.close();

        final ArgumentCaptor<ByteSequence> sequenceCaptor = ArgumentCaptor
                .forClass(ByteSequence.class);
        verify(cursor).writeSequence(sequenceCaptor.capture());
        assertEquals(true, ByteSequences.contentEquals(BYTES_16,
                sequenceCaptor.getValue()));
        verify(cursor, times(1)).writeSequence(any(ByteSequence.class));
    }

    @BeforeEach
    void beforeEach() {
        writer = new CellStoreWriterImpl(cursor);
    }

    @AfterEach
    void afterEach() {
        writer = null;
    }

}
