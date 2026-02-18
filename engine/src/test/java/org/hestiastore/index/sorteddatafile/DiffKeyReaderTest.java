package org.hestiastore.index.sorteddatafile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

        // header not fully read => reader returns null
        when(fileReader.read(any(byte[].class))).thenAnswer(inv -> {
            final byte[] h = (byte[]) inv.getArguments()[0];
            if (h == null) {
                return -1;
            }
            return -1; // simulate EOF on header
        });
        final String ret = reader.read(fileReader);
        assertNull(ret);
    }

    @Test
    void test_first_record_expect_previous() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

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
                tds.getConvertorFromBytes());

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
                tds.getConvertorFromBytes());

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
                tds.getConvertorFromBytes());

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
        final String ret1 = reader.read(fileReader);
        assertEquals("prase", ret1);

        // Second: shared=3, len=5 with diff "lesni" => handled by stateful answer
        final String ret2 = reader.read(fileReader);
        assertEquals("pralesni", ret2);
    }

    @Test
    void test_reading_more_records_with_inconsistency() {
        final DiffKeyReader<String> reader = new DiffKeyReader<>(
                tds.getConvertorFromBytes());

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
                if (buf.length == 5) {
                    if (step == 0) {
                        loadStringToByteArray(inv, "prase");
                        step = 1;
                        return 5;
                    }
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
                tds.getConvertorFromBytes());

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
                    } else {
                        return 3; // partial read for diff
                    }
                }
                return -1;
            }
        });
        final String ret1 = reader.read(fileReader);
        assertEquals("prase", ret1);

        // Second: partial diff read -> error (handled by answer above)
        assertThrows(IndexException.class, () -> reader.read(fileReader));
    }

    private void loadStringToByteArray(final InvocationOnMock invocation,
            final String str) {
        final byte[] bytes = (byte[]) invocation.getArguments()[0];
        final byte[] p = str.getBytes();
        for (int i = 0; i < p.length; i++) {
            bytes[i] = p[i];
        }
    }
}
