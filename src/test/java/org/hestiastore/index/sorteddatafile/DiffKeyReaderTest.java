package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.FileReader;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

class DiffKeyReaderTest {

    private final FileReader fileReader = mock(FileReader.class);

    private final TypeDescriptor<String> tds = new TypeDescriptorShortString();

    @Test
    void test_reading_end_of_file() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(-1);
        final String ret = reader.read(fileReader);
        assertNull(ret);
    }

    @Test
    void test_first_record_expect_previous() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(3).thenReturn(5);
        when(fileReader.read(
                argThat((Bytes bytes) -> bytes != null && bytes.length() == 5)))
                .thenAnswer(invocation -> {
                    loadStringToBytes(invocation, "prase");
                    return 5;
                });
        assertThrows(IndexException.class, () -> reader.read(fileReader));
    }

    @Test
    void test_reading_first_full_record() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(0).thenReturn(5);
        when(fileReader.read(
                argThat((Bytes bytes) -> bytes != null && bytes.length() == 5)))
                .thenAnswer(invocation -> {
                    loadStringToBytes(invocation, "prase");
                    return 5;
                });
        final String ret = reader.read(fileReader);
        assertEquals("prase", ret);
    }

    @Test
    void test_reading_first_fail_when_just_part_of_data_is_read() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(0).thenReturn(5);
        when(fileReader.read(
                argThat((Bytes bytes) -> bytes != null && bytes.length() == 5)))
                .thenAnswer(invocation -> {
                    return 3;
                });
        assertThrows(IndexException.class, () -> reader.read(fileReader));
    }

    @Test
    void test_reading_more_records() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(0).thenReturn(5);
        when(fileReader.read(argThat((Bytes bytes) -> bytes != null
                && Arrays.equals(new byte[5], bytes.getData()))))
                .thenAnswer(invocation -> {
                    loadStringToBytes(invocation, "prase");
                    return 5;
                });
        final String ret1 = reader.read(fileReader);
        assertEquals("prase", ret1);

        when(fileReader.read()).thenReturn(3).thenReturn(5);
        when(fileReader.read(argThat((Bytes bytes) -> bytes != null
                && Arrays.equals(new byte[5], bytes.getData()))))
                .thenAnswer(invocation -> {
                    loadStringToBytes(invocation, "lesni");
                    return 5;
                });
        final String ret2 = reader.read(fileReader);
        assertEquals("pralesni", ret2);
    }

    @Test
    void test_reading_more_records_with_inconsistency() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(0).thenReturn(5);
        when(fileReader.read(
                argThat((Bytes bytes) -> bytes != null && bytes.length() == 5)))
                .thenAnswer(invocation -> {
                    loadStringToBytes(invocation, "prase");
                    return 5;
                });
        final String ret1 = reader.read(fileReader);
        assertEquals("prase", ret1);

        when(fileReader.read()).thenReturn(11).thenReturn(5);
        assertThrows(IndexException.class, () -> reader.read(fileReader));
    }

    @Test
    void test_reading_more_records_second_reading_load_part_of_bytes() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

        when(fileReader.read()).thenReturn(0).thenReturn(5);
        when(fileReader.read(argThat((Bytes bytes) -> bytes != null
                && Arrays.equals(new byte[5], bytes.getData()))))
                .thenAnswer(invocation -> {
                    loadStringToBytes(invocation, "prase");
                    return 5;
                });
        final String ret1 = reader.read(fileReader);
        assertEquals("prase", ret1);

        when(fileReader.read()).thenReturn(3).thenReturn(5);
        when(fileReader.read(argThat((Bytes bytes) -> bytes != null
                && Arrays.equals(new byte[5], bytes.getData())))).thenReturn(3);
        assertThrows(IndexException.class, () -> reader.read(fileReader));
    }

    private void loadStringToBytes(final InvocationOnMock invocation,
            final String str) {
        final Bytes bytes = invocation.getArgument(0, Bytes.class);
        final byte[] data = bytes.getData();
        byte[] p = str.getBytes();
        for (int i = 0; i < 5; i++) {
            data[i] = p[i];
        }
    }
}
