package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.TestData;
import org.hestiastore.index.datablockfile.DataBlockPayload;
import org.hestiastore.index.datablockfile.DataBlockWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class CellStoreWriterCursorTest {

    private static final int DATABLOCK_PAYLOAD_SIZE = 64;
    private static final Bytes BYTES_64 = TestData.BYTES_1024.subBytes(0, 64);
    private static final Bytes BYTES_16 = TestData.BYTES_1024.subBytes(0, 16);
    private static final Bytes BYTES_18 = TestData.BYTES_1024.subBytes(0, 18);
    private static final Bytes BYTES_80 = TestData.BYTES_1024.subBytes(0, 80);
    private static final Bytes BYTES_48 = TestData.BYTES_1024.subBytes(0, 48);

    @Mock
    private DataBlockWriter dataBlockWriter;

    private CellStoreWriterCursor cursor;

    @Test
    void test_no_write() {
        assertEquals(0, cursor.getNextCellPosition().getValue());
        cursor.close();

        // verify that data block write was never called
        verify(dataBlockWriter, never()).write(any(DataBlockPayload.class));
    }

    @Test
    void test_write_64_bytes() {
        assertEquals(64, cursor.write(BYTES_64).getValue());
        cursor.close();

        verify(dataBlockWriter).write(DataBlockPayload.of(BYTES_64));

        // verify that data block write wasn't called more than once
        verify(dataBlockWriter, times(1)).write(any(DataBlockPayload.class));
    }

    @Test
    void test_write_16_bytes() {
        assertEquals(16, cursor.write(BYTES_16).getValue());
        cursor.close();

        ArgumentCaptor<DataBlockPayload> argumentCaptor = ArgumentCaptor
                .forClass(DataBlockPayload.class);

        verify(dataBlockWriter).write(argumentCaptor.capture());
        DataBlockPayload capturedValue = argumentCaptor.getValue();

        assertEquals(BYTES_16, capturedValue.getBytes().subBytes(0, 16));

        // verify that data block write wasn't called more than once
        verify(dataBlockWriter, times(1)).write(any(DataBlockPayload.class));
        assertEquals(16, cursor.getNextCellPosition().getValue());
    }

    @Test
    void test_write_16_and_48_bytes() {
        assertEquals(16, cursor.write(BYTES_16).getValue());
        assertEquals(64, cursor.write(BYTES_48).getValue());
        cursor.close();

        ArgumentCaptor<DataBlockPayload> argumentCaptor = ArgumentCaptor
                .forClass(DataBlockPayload.class);

        verify(dataBlockWriter, times(1)).write(argumentCaptor.capture());
        DataBlockPayload capturedValue = argumentCaptor.getValue();

        assertEquals(BYTES_16, capturedValue.getBytes().subBytes(0, 16));
        assertEquals(BYTES_48, capturedValue.getBytes().subBytes(16, 64));

        // verify that data block write wasn't called more than once
        verify(dataBlockWriter, times(1)).write(any(DataBlockPayload.class));
    }

    @Test
    void test_write_80_bytes_exception() {
        final Exception e = assertThrows(IllegalStateException.class,
                () -> cursor.write(BYTES_80));

        assertEquals("Not enough space to write to data block", e.getMessage());
    }

    @Test
    void test_write_16_64_bytes_exception() {
        cursor.write(BYTES_16);
        final Exception e = assertThrows(IllegalStateException.class,
                () -> cursor.write(BYTES_64));

        assertEquals("Not enough space to write to data block", e.getMessage());
    }

    @Test
    void test_write_bytes_not_divisible_by_16_exception() {
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> cursor.write(BYTES_18));
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
                () -> new CellStoreWriterCursor(null, DATABLOCK_PAYLOAD_SIZE));
    }

    @Test
    void test_write_null_bytes() {
        assertThrows(IllegalArgumentException.class, () -> cursor.write(null));
    }

    @BeforeEach
    void beforeEach() {
        cursor = new CellStoreWriterCursor(dataBlockWriter,
                DATABLOCK_PAYLOAD_SIZE);
    }

    @AfterEach
    void afterEach() {
        cursor = null;
    }

}
