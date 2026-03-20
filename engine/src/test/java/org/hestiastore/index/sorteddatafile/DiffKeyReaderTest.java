package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;

import org.hestiastore.index.AbstractCloseableResource;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.datatype.TypeDescriptor;
import org.hestiastore.index.datatype.TypeDescriptorShortString;
import org.hestiastore.index.datatype.TypeDescriptorString;
import org.hestiastore.index.directory.FileReader;
import org.hestiastore.index.directory.FileWriter;
import org.hestiastore.index.directory.MemFileReader;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

class DiffKeyReaderTest {

    private final FileReader fileReader = mock(FileReader.class);

    private final TypeDescriptor<String> tds = new TypeDescriptorShortString();

    @Test
    void test_reading_end_of_file() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getTypeDecoder());

        // header not fully read => reader returns null
        when(fileReader.read(any(byte[].class)))
                .thenAnswer(invocation -> -1); // simulate EOF on header
        final String ret = reader.read(fileReader);
        assertNull(ret);
    }

    @Test
    void test_first_record_expect_previous() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getTypeDecoder());

        // shared=3, keyLen=5 on the very first key -> invalid (needs previous)
        when(fileReader.read(any(byte[].class))).thenAnswer(inv -> {
            final byte[] buf = (byte[]) inv.getArguments()[0];
            if (buf == null) {
                return -1;
            }
            if (buf.length == 2) {
                buf[0] = 3;
                buf[1] = 5;
                return 2;
            }
            if (buf.length == 5) {
                loadStringToByteArray(inv, "prase");
                return 5;
            }
            return -1;
        });
        assertThrows(IndexException.class, () -> reader.read(fileReader));
    }

    @Test
    void test_reading_first_full_record() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getTypeDecoder());

        // shared=0, keyLen=5 -> full key "prase"
        when(fileReader.read(any(byte[].class))).thenAnswer(inv -> {
            final byte[] buf = (byte[]) inv.getArguments()[0];
            if (buf.length == 2) {
                buf[0] = 0;
                buf[1] = 5;
                return 2;
            }
            if (buf.length == 5) {
                loadStringToByteArray(inv, "prase");
                return 5;
            }
            return -1;
        });
        final String ret = reader.read(fileReader);
        assertEquals("prase", ret);
    }

    @Test
    void test_reading_first_fail_when_just_part_of_data_is_read() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getTypeDecoder());

        // shared=0, keyLen=5 then partial payload read
        when(fileReader.read(any(byte[].class))).thenAnswer(inv -> {
            final byte[] buf = (byte[]) inv.getArguments()[0];
            if (buf.length == 2) {
                buf[0] = 0;
                buf[1] = 5;
                return 2;
            }
            if (buf.length == 5) {
                return 3; // simulate partial read
            }
            return -1;
        });
        assertThrows(IndexException.class, () -> reader.read(fileReader));
    }

    @Test
    void test_reading_more_records() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getTypeDecoder());

        // Use stateful answer to simulate two records
        when(fileReader.read(any(byte[].class))).thenAnswer(new org.mockito.stubbing.Answer<Integer>() {
            int step = 0;
            @Override
            public Integer answer(org.mockito.invocation.InvocationOnMock inv) {
                final byte[] buf = (byte[]) inv.getArguments()[0];
                if (buf.length == 2) {
                    if (step == 0) {
                        buf[0] = 0; buf[1] = 5; // first record full
                    } else {
                        buf[0] = 3; buf[1] = 5; // second record shares 3 bytes
                    }
                    return 2;
                }
                if (buf.length == 5) {
                    if (step == 0) {
                        loadStringToByteArray(inv, "prase");
                        step = 1;
                        return 5;
                    } else {
                        loadStringToByteArray(inv, "lesni");
                        step = 2;
                        return 5;
                    }
                }
                return -1;
            }
        });
        when(fileReader.read(any(byte[].class), anyInt(), anyInt()))
                .thenAnswer(new org.mockito.stubbing.Answer<Integer>() {
                    @Override
                    public Integer answer(
                            org.mockito.invocation.InvocationOnMock inv) {
                        final byte[] buf = (byte[]) inv.getArguments()[0];
                        final int offset = (Integer) inv.getArguments()[1];
                        final int length = (Integer) inv.getArguments()[2];
                        if (length == 5) {
                            final byte[] p = "lesni".getBytes();
                            System.arraycopy(p, 0, buf, offset, p.length);
                            return 5;
                        }
                        return -1;
                    }
                });
        final String ret1 = reader.read(fileReader);
        assertEquals("prase", ret1);

        // Second: shared=3, len=5 with diff "lesni" => handled by stateful answer
        final String ret2 = reader.read(fileReader);
        assertEquals("pralesni", ret2);
    }

    @Test
    void test_reading_more_records_with_inconsistency() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getTypeDecoder());

        // First full key
        when(fileReader.read(any(byte[].class))).thenAnswer(new org.mockito.stubbing.Answer<Integer>() {
            int step = 0;
            @Override
            public Integer answer(org.mockito.invocation.InvocationOnMock inv) {
                final byte[] buf = (byte[]) inv.getArguments()[0];
                if (buf.length == 2) {
                    if (step == 0) {
                        buf[0] = 0; buf[1] = 5;
                        return 2;
                    } else {
                        buf[0] = 11; buf[1] = 5; // inconsistent shared length
                        return 2;
                    }
                }
                if (buf.length == 5 && step == 0) {
                    loadStringToByteArray(inv, "prase");
                    step = 1;
                    return 5;
                }
                return -1;
            }
        });
        final String ret1 = reader.read(fileReader);
        assertEquals("prase", ret1);

        // Second header claims more shared bytes than previous key length -> error
        assertThrows(IndexException.class, () -> reader.read(fileReader));
    }

    @Test
    void test_reading_more_records_second_reading_load_part_of_bytes() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getTypeDecoder());

        // First full key
        when(fileReader.read(any(byte[].class))).thenAnswer(new org.mockito.stubbing.Answer<Integer>() {
            int step = 0;
            @Override
            public Integer answer(org.mockito.invocation.InvocationOnMock inv) {
                final byte[] buf = (byte[]) inv.getArguments()[0];
                if (buf.length == 2) {
                    if (step == 0) {
                        buf[0] = 0; buf[1] = 5; return 2;
                    } else {
                        buf[0] = 3; buf[1] = 5; return 2; // second header
                    }
                }
                if (buf.length == 5) {
                    if (step == 0) {
                        loadStringToByteArray(inv, "prase");
                        step = 1;
                        return 5;
                    }
                    return 3; // partial read for diff
                }
                return -1;
            }
        });
        when(fileReader.read(any(byte[].class), anyInt(), anyInt()))
                .thenAnswer(new org.mockito.stubbing.Answer<Integer>() {
                    @Override
                    public Integer answer(
                            org.mockito.invocation.InvocationOnMock inv) {
                        final int length = (Integer) inv.getArguments()[2];
                        if (length == 5) {
                            return 3; // partial read for diff
                        }
                        return -1;
                    }
                });
        final String ret1 = reader.read(fileReader);
        assertEquals("prase", ret1);

        // Second: partial diff read -> error (handled by answer above)
        assertThrows(IndexException.class, () -> reader.read(fileReader));
    }

    @Test
    void test_reading_unsigned_header_lengths() {
        final TypeDescriptor<String> longStringDescriptor = new TypeDescriptorString();
        final DiffKeyWriter<String> writer = new DiffKeyWriter<>(
                longStringDescriptor.getTypeEncoder(),
                Comparator.naturalOrder());
        final String firstKey = "a".repeat(130);
        final String secondKey = firstKey + "b".repeat(5);
        final byte[] encoded = concat(writeSingle(writer, firstKey),
                writeSingle(writer, secondKey));
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                longStringDescriptor.getTypeDecoder());

        try (MemFileReader memFileReader = new MemFileReader(encoded)) {
            assertEquals(firstKey, reader.read(memFileReader));
            assertEquals(secondKey, reader.read(memFileReader));
            assertNull(reader.read(memFileReader));
        }
    }

    private void loadStringToByteArray(final InvocationOnMock invocation,
            final String str) {
        final byte[] bytes = (byte[]) invocation.getArguments()[0];
        final byte[] p = str.getBytes(StandardCharsets.ISO_8859_1);
        for (int i = 0; i < p.length; i++) {
            bytes[i] = p[i];
        }
    }

    private static <K> byte[] writeSingle(final DiffKeyWriter<K> writer,
            final K key) {
        final CollectingFileWriter collectingWriter = new CollectingFileWriter();
        final int written = writer.writeTo(collectingWriter, key);
        final byte[] encoded = collectingWriter.toByteArray();
        assertEquals(encoded.length, written);
        return encoded;
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
