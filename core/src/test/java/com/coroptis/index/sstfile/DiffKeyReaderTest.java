package com.coroptis.index.sstfile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import com.coroptis.index.IndexException;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorString;
import com.coroptis.index.directory.FileReader;

public class DiffKeyReaderTest {

    private final FileReader fileReader = mock(FileReader.class);

    private final TypeDescriptor<String> tds = new TypeDescriptorString();

    @Test
    public void test_reading_end_of_file() throws Exception {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(-1);
        final String ret = reader.read(fileReader);
        assertNull(ret);
    }

    @Test
    public void test_first_record_expect_previous() throws Exception {
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
    public void test_reading_first_full_record() throws Exception {
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
    public void test_reading_first_fail_when_just_part_of_data_is_read()
            throws Exception {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(0).thenReturn(5);
        when(fileReader.read(eq(new byte[5]))).thenAnswer(invocation -> {
            return 3;
        });
        assertThrows(IndexException.class, () -> reader.read(fileReader));
    }

    @Test
    public void test_reading_more_records() throws Exception {
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
    public void test_reading_more_records_with_inconsistency()
            throws Exception {
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
    public void test_reading_more_records_second_reading_load_part_of_bytes()
            throws Exception {
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
//                    loadStringToByteArray(invocation, "lesni");
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
