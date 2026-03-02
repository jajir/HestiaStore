package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.util.Comparator;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.datatype.EncodedBytes;
import org.hestiastore.index.datatype.TypeEncoder;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorInteger;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.directory.FileWriter;
import org.junit.jupiter.api.Test;

class DiffKeyWriterTest {

    private final TypeDescriptor<Integer> tdi = new TypeDescriptorInteger();
    private final TypeDescriptor<String> tds = new TypeDescriptorShortString();

    private DiffKeyWriter<Integer> makeDiffKeyWriter() {
        return new DiffKeyWriter<>(tdi.getTypeEncoder(),
                Comparator.naturalOrder());
    }

    @Test
    void test_ordering_of_key() {
        final DiffKeyWriter<Integer> diffWriter = makeDiffKeyWriter();
        writeSingle(diffWriter, 1);
        writeSingle(diffWriter, 2);
        writeSingle(diffWriter, 3);
        writeSingle(diffWriter, 4);

        // if no exception is thrown, then correct was accepted
        assertTrue(true);
    }

    @Test
    void test_ordering_same_keys_throw_exception() {
        DiffKeyWriter<Integer> diffWriter = makeDiffKeyWriter();
        writeSingle(diffWriter, 1);
        diffWriter = makeDiffKeyWriter();
        writeSingle(diffWriter, 2);
        diffWriter = makeDiffKeyWriter();
        writeSingle(diffWriter, 3);

        final DiffKeyWriter<Integer> diffWriter2 = diffWriter;
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> writeSingle(diffWriter2, 3));

        assertTrue(e.getMessage()
                .startsWith("Attempt to insers same key as previous. Key"));
    }

    @Test
    void test_ordering_same_keys_throw_full_write_exception() {
        DiffKeyWriter<Integer> diffWriter = makeDiffKeyWriter();
        writeSingle(diffWriter, 1);
        diffWriter = makeDiffKeyWriter();
        writeSingle(diffWriter, 2);
        diffWriter = makeDiffKeyWriter();
        writeSingle(diffWriter, 3);

        final DiffKeyWriter<Integer> diffWriter2 = makeDiffKeyWriter();

        // nothing is thrown because new class is created
        writeSingle(diffWriter2, 3);
        assertTrue(true);
    }

    @Test
    void test_ordering_smaller_key_than_previous_one_throw_exception() {
        DiffKeyWriter<Integer> diffWriter = makeDiffKeyWriter();
        writeSingle(diffWriter, 1);

        assertThrows(IllegalArgumentException.class,
                () -> writeSingle(diffWriter, -1));
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
        final TypeEncoder<Integer> convertorToBytes = tdi
                .getTypeEncoder();
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> new DiffKeyWriter<Integer>(convertorToBytes, null));

        assertEquals("Property 'keyComparator' must not be null.",
                e.getMessage());
    }

    @Test
    void test_write() {
        assertTrue(true);
        DiffKeyWriter<String> diffWriter = new DiffKeyWriter<>(
                tds.getTypeEncoder(), Comparator.naturalOrder());

        byte[] ret = writeSingle(diffWriter, "aaa");
        verifyDiffKey(0, 3, "aaa", ret);

        ret = writeSingle(diffWriter, "bbb");
        verifyDiffKey(0, 3, "bbb", ret);

        ret = writeSingle(diffWriter, "bbc");
        verifyDiffKey(2, 1, "c", ret);

        ret = writeSingle(diffWriter, "bcc");
        verifyDiffKey(1, 2, "cc", ret);
    }

    @Test
    void test_close_returns_zero() {
        final DiffKeyWriter<Integer> diffWriter = makeDiffKeyWriter();

        assertEquals(0, diffWriter.close());
    }

    @Test
    void test_writeTo_writes_expected_payload() {
        final DiffKeyWriter<String> expectedWriter = new DiffKeyWriter<>(
                tds.getTypeEncoder(), Comparator.naturalOrder());
        final DiffKeyWriter<String> writerUsingFileWriter = new DiffKeyWriter<>(
                tds.getTypeEncoder(), Comparator.naturalOrder());
        final CollectingFileWriter collectingWriter = new CollectingFileWriter();

        final byte[] expectedA = writeSingle(expectedWriter, "aaa");
        final byte[] expectedB = writeSingle(expectedWriter, "bbc");

        final int writtenA = writerUsingFileWriter.writeTo(collectingWriter,
                "aaa");
        final int writtenB = writerUsingFileWriter.writeTo(collectingWriter,
                "bbc");

        final byte[] expected = concat(expectedA, expectedB);
        assertEquals(expected.length, writtenA + writtenB);
        assertArrayEquals(expected, collectingWriter.toByteArray());
    }

    @Test
    void test_write_propagatesEncoderValidationErrors() {
        final DiffKeyWriter<Integer> diffWriter = new DiffKeyWriter<>(
                new TypeEncoder<Integer>() {
                    @Override
                    public EncodedBytes encode(final Integer value,
                            final byte[] reusableBuffer) {
                        throw new IllegalArgumentException("bad diff key");
                    }
                }, Comparator.naturalOrder());

        final IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> writeSingle(diffWriter, 1));
        assertEquals("bad diff key", error.getMessage());
    }

    @Test
    void test_write_propagatesEncoderIllegalState() {
        final DiffKeyWriter<Integer> diffWriter = new DiffKeyWriter<>(
                new TypeEncoder<Integer>() {
                    @Override
                    public EncodedBytes encode(final Integer value,
                            final byte[] reusableBuffer) {
                        throw new IllegalStateException("inconsistent key");
                    }
                }, Comparator.naturalOrder());

        final IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> writeSingle(diffWriter, 1));
        assertEquals("inconsistent key", error.getMessage());
    }

    private static <K> byte[] writeSingle(final DiffKeyWriter<K> writer,
            final K key) {
        final CollectingFileWriter collectingWriter = new CollectingFileWriter();
        final int written = writer.writeTo(collectingWriter, key);
        final byte[] encoded = collectingWriter.toByteArray();
        assertEquals(encoded.length, written);
        return encoded;
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

    private static byte[] concat(final byte[] first, final byte[] second) {
        final byte[] out = new byte[first.length + second.length];
        System.arraycopy(first, 0, out, 0, first.length);
        System.arraycopy(second, 0, out, first.length, second.length);
        return out;
    }

    private static final class CollectingFileWriter
            extends AbstractCloseableResource implements FileWriter {

        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        @Override
        public void write(final byte b) {
            out.write(b);
        }

        @Override
        public void write(final byte[] bytes) {
            out.write(bytes, 0, bytes.length);
        }

        @Override
        public void write(final byte[] bytes, final int offset,
                final int length) {
            out.write(bytes, offset, length);
        }

        byte[] toByteArray() {
            return out.toByteArray();
        }

        @Override
        protected void doClose() {
            // no-op
        }
    }

}
