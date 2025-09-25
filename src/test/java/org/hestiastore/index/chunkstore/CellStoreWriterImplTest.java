package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.TestData;
import org.hestiastore.index.datablockfile.CellPosition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class CellStoreWriterImplTest {

    private static final CellPosition CELL_POSITION_0 = CellPosition
            .of(TestData.DATA_BLOCK_SIZE, 0);
    private static final Bytes BYTES_64 = TestData.BYTES_1024.subBytes(0, 64);
    private static final Bytes BYTES_16 = TestData.BYTES_1024.subBytes(0, 16);

    @Mock
    private CellStoreWriterCursor cursor;

    private CellStoreWriter writer;

    @Test
    void test_write_64_bytes() {
        when(cursor.getAvailableBytes()).thenReturn(64);
        when(cursor.getNextCellPosition()).thenReturn(CELL_POSITION_0);
        assertEquals(0, writer.write(BYTES_64).getValue());
        writer.close();

        // verify that cursor write was called once with 64 bytes
        verify(cursor).write(BYTES_64);

        // verify that cursor write wasn't called more than once
        verify(cursor, times(1)).write(any(Bytes.class));
    }

    @Test
    void test_write_16_bytes() {
        when(cursor.getAvailableBytes()).thenReturn(64);
        when(cursor.getNextCellPosition()).thenReturn(CELL_POSITION_0);
        assertEquals(0, writer.write(BYTES_16).getValue());
        writer.close();

        verify(cursor).write(BYTES_16);
        verify(cursor, times(1)).write(any(Bytes.class));
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
