package com.coroptis.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Comparator;

import org.junit.jupiter.api.Test;

import com.coroptis.index.LoggingContext;
import com.coroptis.index.datatype.TypeDescriptor;
import com.coroptis.index.datatype.TypeDescriptorInteger;
import com.coroptis.index.datatype.TypeDescriptorString;

public class DiffKeyWriterTest {

    private final static LoggingContext LOGGING_CONTEXT = new LoggingContext(
            "test_index");
    private final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private final TypeDescriptor<String> tds = new TypeDescriptorString();

    private DiffKeyWriter<Integer> makeDiffKeyWriter() {
        return new DiffKeyWriter<>(LOGGING_CONTEXT, tdi.getConvertorToBytes(),
                Comparator.naturalOrder());
    }

    @Test
    public void test_ordering_of_key() throws Exception {
        final DiffKeyWriter<Integer> diffWriter = makeDiffKeyWriter();
        diffWriter.write(1);
        diffWriter.write(2);
        diffWriter.write(3);
        diffWriter.write(4);
    }

    @Test
    public void test_ordering_same_keys_throw_exception() throws Exception {
        DiffKeyWriter<Integer> diffWriter = makeDiffKeyWriter();
        diffWriter.write(1);
        diffWriter = makeDiffKeyWriter();
        diffWriter.write(2);
        diffWriter = makeDiffKeyWriter();
        diffWriter.write(3);

        final DiffKeyWriter<Integer> diffWriter2 = diffWriter;
        assertThrows(IllegalArgumentException.class,
                () -> diffWriter2.write(3));
    }

    @Test
    public void test_ordering_same_keys_throw_full_write_exception()
            throws Exception {
        DiffKeyWriter<Integer> diffWriter = makeDiffKeyWriter();
        diffWriter.write(1);
        diffWriter = makeDiffKeyWriter();
        diffWriter.write(2);
        diffWriter = makeDiffKeyWriter();
        diffWriter.write(3);

        final DiffKeyWriter<Integer> diffWriter2 = makeDiffKeyWriter();
        // nothing is thrown because new class is created
        diffWriter2.write(3);
    }

    @Test
    public void test_ordering_smaller_key_than_previous_one_throw_exception()
            throws Exception {
        DiffKeyWriter<Integer> diffWriter = makeDiffKeyWriter();
        diffWriter.write(1);

        assertThrows(IllegalArgumentException.class,
                () -> diffWriter.write(-1));
    }

    @Test
    public void test_constructor_convertorToBytes_is_null() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new DiffKeyWriter<Integer>(LOGGING_CONTEXT, null,
                        Comparator.naturalOrder()));

        assertEquals("Convertor to bytes is null", e.getMessage());
    }

    @Test
    public void test_constructor_comparator_is_null() {
        final Exception e = assertThrows(NullPointerException.class,
                () -> new DiffKeyWriter<Integer>(LOGGING_CONTEXT,
                        tdi.getConvertorToBytes(), null));

        assertEquals("Key comparator can't be null", e.getMessage());
    }

    @Test
    public void test_write() throws Exception {
        DiffKeyWriter<String> diffWriter = new DiffKeyWriter<>(LOGGING_CONTEXT,
                tds.getConvertorToBytes(), Comparator.naturalOrder());

        byte[] ret = diffWriter.write("aaa");
        verifyDiffKey(0, 3, "aaa", ret);

        ret = diffWriter.write("bbb");
        verifyDiffKey(0, 3, "bbb", ret);

        ret = diffWriter.write("bbc");
        verifyDiffKey(2, 1, "c", ret);

        ret = diffWriter.write("bcc");
        verifyDiffKey(1, 2, "cc", ret);
    }

    private void verifyDiffKey(final int expectedSharedByteLength,
            final int expectedBytesLength, final String expectedString,
            final byte[] bytes) {
        assertEquals(expectedSharedByteLength, (int) bytes[0],
                "shared byte length");
        assertEquals(expectedBytesLength, (int) bytes[1], "byte length");
        byte[] b = new byte[bytes.length - 2];
        System.arraycopy(bytes, 2, b, 0, b.length);
        String str = new String(b);
        assertEquals(expectedString, str, "string");
    }

}
