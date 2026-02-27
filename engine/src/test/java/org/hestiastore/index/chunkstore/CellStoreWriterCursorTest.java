package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.TestData;
import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.datablockfile.DataBlockSize;
import org.hestiastore.index.datablockfile.DataBlockWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CellStoreWriterCursorTest {

    private static final DataBlockSize DATABLOCK_SIZE = DataBlockSize
            .ofDataBlockSize(1024);
    private static final ByteSequence BYTES_1008 = TestData.BYTES_1024.slice(0,
            1008);
    private static final ByteSequence BYTES_16 = TestData.BYTES_1024.slice(0,
            16);
    private static final ByteSequence BYTES_18 = TestData.BYTES_1024.slice(0,
            18);
    private static final ByteSequence BYTES_1024 = TestData.BYTES_1024;
    private static final ByteSequence BYTES_48 = TestData.BYTES_1024.slice(0,
            48);

    @Mock
    private DataBlockWriter dataBlockWriter;

    private CellStoreWriterCursor cursor;

    @Test
    void test_no_write() {
        assertEquals(0, cursor.getNextCellPosition().getValue());
        cursor.close();

        // verify that data block write was never called
        verify(dataBlockWriter, never()).writeSequence(any(ByteSequence.class));
    }

    @Test
    void test_write_1008_bytes() {
        assertEquals(1008, cursor.writeSequence(BYTES_1008).getValue());
        cursor.close();

        final ArgumentCaptor<ByteSequence> argumentCaptor = ArgumentCaptor
                .forClass(ByteSequence.class);
        verify(dataBlockWriter).writeSequence(argumentCaptor.capture());
        assertEquals(true, ByteSequences.contentEquals(BYTES_1008,
                argumentCaptor.getValue()));

        // verify that data block write wasn't called more than once
        verify(dataBlockWriter, times(1))
                .writeSequence(any(ByteSequence.class));
        assertEquals(1008, cursor.getAvailableBytes());
    }

    @Test
    void test_write_16_bytes() {
        assertEquals(16, cursor.writeSequence(BYTES_16).getValue());
        cursor.close();

        ArgumentCaptor<ByteSequence> argumentCaptor = ArgumentCaptor
                .forClass(ByteSequence.class);

        verify(dataBlockWriter).writeSequence(argumentCaptor.capture());
        final ByteSequence capturedValue = argumentCaptor.getValue();
        assertEquals(true, ByteSequences.contentEquals(BYTES_16,
                capturedValue.slice(0, 16)));

        // verify that data block write wasn't called more than once
        verify(dataBlockWriter, times(1))
                .writeSequence(any(ByteSequence.class));
        assertEquals(16, cursor.getNextCellPosition().getValue());
        assertEquals(992, cursor.getAvailableBytes());
    }

    @Test
    void test_write_16_and_48_bytes() {
        assertEquals(16, cursor.writeSequence(BYTES_16).getValue());
        assertEquals(64, cursor.writeSequence(BYTES_48).getValue());
        cursor.close();

        ArgumentCaptor<ByteSequence> argumentCaptor = ArgumentCaptor
                .forClass(ByteSequence.class);

        verify(dataBlockWriter, times(1))
                .writeSequence(argumentCaptor.capture());
        final ByteSequence capturedValue = argumentCaptor.getValue();
        assertEquals(true, ByteSequences.contentEquals(BYTES_16,
                capturedValue.slice(0, 16)));
        assertEquals(true, ByteSequences.contentEquals(BYTES_48,
                capturedValue.slice(16, 64)));

        // verify that data block write wasn't called more than once
        verify(dataBlockWriter, times(1))
                .writeSequence(any(ByteSequence.class));
        assertEquals(944, cursor.getAvailableBytes());
    }

    @Test
    void test_write_1024_bytes_exception() {
        final Exception e = assertThrows(IllegalStateException.class,
                () -> cursor.writeSequence(BYTES_1024));

        assertEquals("Not enough space to write to data block", e.getMessage());
    }

    @Test
    void test_write_16_1024_bytes_exception() {
        cursor.writeSequence(BYTES_16);
        final Exception e = assertThrows(IllegalStateException.class,
                () -> cursor.writeSequence(BYTES_1024));

        assertEquals("Not enough space to write to data block", e.getMessage());
    }

    @Test
    void test_write_bytes_not_divisible_by_16_exception() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> cursor.writeSequence(BYTES_18));
        assertEquals(
                "Property 'bytes' must be divisible by 16 (e.g., 16, 32, 64). Got: '18'",
                e.getMessage());
    }

    @Test
    void test_close() {
        cursor.close();
        // verify that data block close was called once
        verify(dataBlockWriter, times(1)).close();
    }

    @Test
    void test_constructor_null_dataBlockWriter() {
        assertThrows(IllegalArgumentException.class,
                () -> new CellStoreWriterCursor(null, DATABLOCK_SIZE));
    }

    @Test
    void test_constructor_null_dataBlockSize() {
        assertThrows(IllegalArgumentException.class,
                () -> new CellStoreWriterCursor(dataBlockWriter, null));
    }

    @Test
    void test_write_null_bytes() {
        assertThrows(IllegalArgumentException.class,
                () -> cursor.writeSequence(null));
    }

    @BeforeEach
    void beforeEach() {
        cursor = new CellStoreWriterCursor(dataBlockWriter, DATABLOCK_SIZE);
    }

    @AfterEach
    void afterEach() {
        cursor = null;
    }

}
