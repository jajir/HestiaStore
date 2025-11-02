package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.datatype.ConvertorToBytes;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.junit.jupiter.api.Test;

class DiffKeyWriterTest {

    private final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private final TypeDescriptor<String> tds = new TypeDescriptorShortString();

    private DiffKeyWriter<Integer> makeDiffKeyWriter() {
        return new DiffKeyWriter<>(tdi.getConvertorToBytes(),
                Comparator.naturalOrder());
    }

    @Test
    void test_ordering_of_key() {
        final DiffKeyWriter<Integer> diffWriter = makeDiffKeyWriter();
        diffWriter.write(1);
        diffWriter.write(2);
        diffWriter.write(3);
        diffWriter.write(4);

        // if no exception is thrown, then correct was accepted
        assertTrue(true);
    }

    @Test
    void test_ordering_same_keys_throw_exception() {
        DiffKeyWriter<Integer> diffWriter = makeDiffKeyWriter();
        diffWriter.write(1);
        diffWriter = makeDiffKeyWriter();
        diffWriter.write(2);
        diffWriter = makeDiffKeyWriter();
        diffWriter.write(3);

        final DiffKeyWriter<Integer> diffWriter2 = diffWriter;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> diffWriter2.write(3));

        assertTrue(e.getMessage()
                .startsWith("Attempt to insers same key as previous. Key"));
    }

    @Test
    void test_ordering_same_keys_throw_full_write_exception() {
        DiffKeyWriter<Integer> diffWriter = makeDiffKeyWriter();
        diffWriter.write(1);
        diffWriter = makeDiffKeyWriter();
        diffWriter.write(2);
        diffWriter = makeDiffKeyWriter();
        diffWriter.write(3);

        final DiffKeyWriter<Integer> diffWriter2 = makeDiffKeyWriter();

        // nothing is thrown because new class is created
        diffWriter2.write(3);
        assertTrue(true);
    }

    @Test
    void test_ordering_smaller_key_than_previous_one_throw_exception() {
        DiffKeyWriter<Integer> diffWriter = makeDiffKeyWriter();
        diffWriter.write(1);

        assertThrows(IllegalArgumentException.class,
                () -> diffWriter.write(-1));
    }

    @Test
    void test_constructor_convertorToBytes_is_null() {
        final Comparator<Integer> comparator = Comparator.naturalOrder();
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new DiffKeyWriter<Integer>(null, comparator));

        assertEquals("Property 'convertorToBytes' must not be null.",
                e.getMessage());
    }

    @Test
    void test_constructor_comparator_is_null() {
        final ConvertorToBytes<Integer> convertorToBytes = tdi
                .getConvertorToBytes();
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new DiffKeyWriter<Integer>(convertorToBytes, null));

        assertEquals("Property 'keyComparator' must not be null.",
                e.getMessage());
    }

    @Test
    void test_write() {
        DiffKeyWriter<String> diffWriter = new DiffKeyWriter<>(
                tds.getConvertorToBytes(), Comparator.naturalOrder());

        ByteSequence ret = diffWriter.write("aaa");
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
            final ByteSequence bytes) {
        assertEquals(expectedSharedByteLength,
                Byte.toUnsignedInt(bytes.getByte(0)), "shared byte length");
        assertEquals(expectedBytesLength, Byte.toUnsignedInt(bytes.getByte(1)),
                "byte length");
        byte[] b = new byte[bytes.length() - 2];
        bytes.copyTo(2, b, 0, b.length);
        String str = new String(b);
        assertEquals(expectedString, str, "string");
    }

}
