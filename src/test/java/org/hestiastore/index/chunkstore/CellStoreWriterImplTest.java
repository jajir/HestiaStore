package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import java.util.List;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.ByteSequenceView;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.TestData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
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

        final ArgumentCaptor<ByteSequence> captor = ArgumentCaptor
                .forClass(ByteSequence.class);
        verify(cursor, times(1)).write(captor.capture());
        assertEquals(64, captor.getValue().length());
        assertArrayEquals(BYTES_64.toByteArray(), toArray(captor.getValue()));
    }

    @Test
    void test_write_16_bytes() {
        when(cursor.getAvailableBytes()).thenReturn(64);
        when(cursor.getNextCellPosition()).thenReturn(CELL_POSITION_0);
        assertEquals(0, writer.write(BYTES_16).getValue());
        writer.close();

        final ArgumentCaptor<ByteSequence> captor = ArgumentCaptor
                .forClass(ByteSequence.class);
        verify(cursor, times(1)).write(captor.capture());
        assertEquals(16, captor.getValue().length());
        assertArrayEquals(BYTES_16.toByteArray(), toArray(captor.getValue()));
    }

    @Test
    void test_write_with_byte_sequence_view() {
        when(cursor.getAvailableBytes()).thenReturn(64);
        when(cursor.getNextCellPosition()).thenReturn(CELL_POSITION_0);

        final ByteSequence sequence = ByteSequenceView
                .of(TestData.BYTE_ARRAY_1024, 0, 64);

        writer.write(sequence);

        final ArgumentCaptor<ByteSequence> captor = ArgumentCaptor
                .forClass(ByteSequence.class);
        verify(cursor).write(captor.capture());
        assertFalse(captor.getValue() instanceof Bytes);
    }

    @Test
    void test_write_spans_multiple_blocks() {
        when(cursor.getAvailableBytes()).thenReturn(32, 16, 64);
        when(cursor.getNextCellPosition()).thenReturn(CELL_POSITION_0);

        final ByteSequence data = TestData.BYTES_1024.slice(0, 96);

        writer.write(data);

        final ArgumentCaptor<ByteSequence> captor = ArgumentCaptor
                .forClass(ByteSequence.class);
        verify(cursor, times(3)).write(captor.capture());

        final List<Integer> chunkLengths = captor.getAllValues().stream()
                .map(ByteSequence::length).toList();
        assertEquals(List.of(32, 16, 48), chunkLengths);
    }

    @Test
    void test_write_throws_when_cursor_reports_no_space() {
        when(cursor.getAvailableBytes()).thenReturn(0);
        when(cursor.getNextCellPosition()).thenReturn(CELL_POSITION_0);

        final ByteSequence data = TestData.BYTES_1024.slice(0, 16);

        assertThrows(IllegalStateException.class, () -> writer.write(data));
        verify(cursor, never()).write(any(ByteSequence.class));
    }

    @BeforeEach
    void beforeEach() {
        writer = new CellStoreWriterImpl(cursor);
    }

    @AfterEach
    void afterEach() {
        writer = null;
    }

    private static byte[] toArray(final ByteSequence sequence) {
        final byte[] array = new byte[sequence.length()];
        if (array.length > 0) {
            sequence.copyTo(0, array, 0, array.length);
        }
        return array;
    }

}
