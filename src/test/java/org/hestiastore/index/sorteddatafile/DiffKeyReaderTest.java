package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.FileReader;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

public class DiffKeyReaderTest {

    private final FileReader fileReader = mock(FileReader.class);

    private final TypeDescriptor<String> tds = new TypeDescriptorString();

    @Test
    public void test_reading_end_of_file() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(-1);
        final String ret = reader.read(fileReader);
        assertNull(ret);
    }

    @Test
    public void test_first_record_expect_previous() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(3).thenReturn(5);
        when(fileReader.read(eq(new byte[5]))).thenAnswer(invocation -> {
            loadStringToByteArray(invocation, "prase");
            return 5;
        });
        assertThrows(IndexException.class, () -> reader.read(fileReader));
    }

    @Test
    public void test_reading_first_full_record() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(0).thenReturn(5);
        when(fileReader.read(eq(new byte[5]))).thenAnswer(invocation -> {
            loadStringToByteArray(invocation, "prase");
            return 5;
        });
        final String ret = reader.read(fileReader);
        assertEquals("prase", ret);
    }

    @Test
    public void test_reading_first_fail_when_just_part_of_data_is_read() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(0).thenReturn(5);
        when(fileReader.read(eq(new byte[5]))).thenAnswer(invocation -> {
            return 3;
        });
        assertThrows(IndexException.class, () -> reader.read(fileReader));
    }

    @Test
    public void test_reading_more_records() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(0).thenReturn(5);
        when(fileReader.read(eq(new byte[5]))).thenAnswer(invocation -> {
            loadStringToByteArray(invocation, "prase");
            return 5;
        });
        final String ret1 = reader.read(fileReader);
        assertEquals("prase", ret1);

        when(fileReader.read()).thenReturn(3).thenReturn(5);
        when(fileReader.read(eq(new byte[5]))).thenAnswer(invocation -> {
            loadStringToByteArray(invocation, "lesni");
            return 5;
        });
        final String ret2 = reader.read(fileReader);
        assertEquals("pralesni", ret2);
    }

    @Test
    public void test_reading_more_records_with_inconsistency() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(0).thenReturn(5);
        when(fileReader.read(eq(new byte[5]))).thenAnswer(invocation -> {
            loadStringToByteArray(invocation, "prase");
            return 5;
        });
        final String ret1 = reader.read(fileReader);
        assertEquals("prase", ret1);

        when(fileReader.read()).thenReturn(11).thenReturn(5);
        assertThrows(IndexException.class, () -> reader.read(fileReader));
    }

    @Test
    public void test_reading_more_records_second_reading_load_part_of_bytes() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(0).thenReturn(5);
        when(fileReader.read(eq(new byte[5]))).thenAnswer(invocation -> {
            loadStringToByteArray(invocation, "prase");
            return 5;
        });
        final String ret1 = reader.read(fileReader);
        assertEquals("prase", ret1);

        when(fileReader.read()).thenReturn(3).thenReturn(5);
        when(fileReader.read(eq(new byte[5]))).thenAnswer(invocation -> {
            // loadStringToByteArray(invocation, "lesni");
            return 3;
        });
        assertThrows(IndexException.class, () -> reader.read(fileReader));
    }

    private void loadStringToByteArray(final InvocationOnMock invocation,
            final String str) {
        final byte[] bytes = (byte[]) invocation.getArguments()[0];
        byte[] p = str.getBytes();
        for (int i = 0; i < 5; i++) {
            bytes[i] = p[i];
        }
    }
}
