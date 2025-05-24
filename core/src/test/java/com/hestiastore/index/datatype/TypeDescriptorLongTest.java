package com.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;

import org.junit.jupiter.api.Test;

import com.hestiastore.index.directory.Directory;
import com.hestiastore.index.directory.MemDirectory;

public class TypeDescriptorLongTest {

    private final TypeDescriptorLong ti = new TypeDescriptorLong();
    private final ConvertorToBytes<Long> toBytes = ti.getConvertorToBytes();
    private final ConvertorFromBytes<Long> fromBytes = ti
            .getConvertorFromBytes();

    @Test
    public void test_convertorto_bytes() throws Exception {
        assertEqualsBytes(0L);
        assertEqualsBytes(21L);
        assertEqualsBytes(Long.MAX_VALUE);
        assertEqualsBytes(Long.MIN_VALUE);
        assertEqualsBytes(-1L);
    }

    private void assertEqualsBytes(Long number) {
        final byte[] bytes = toBytes.toBytes(number);
        final Long ret = fromBytes.fromBytes(bytes);
        assertEquals(number, ret, String
                .format("Expected '%s' byt returned was '%s'", number, ret));
    }

    @Test
    public void test_writer() throws Exception {
        Directory dir = new MemDirectory();

        int ret = ti.getTypeWriter().write(dir.getFileWriter("test"),
                Long.MIN_VALUE);

        assertEquals(8, ret);
    }

    @Test
    public void test_compare() throws Exception {
        final Comparator<Long> cmp = ti.getComparator();
        assertTrue(cmp.compare(0l, 0L) == 0);
        assertTrue(cmp.compare(3l, 12L) < 0);
        assertTrue(cmp.compare(3l, 2L) > 0);
    }
}
