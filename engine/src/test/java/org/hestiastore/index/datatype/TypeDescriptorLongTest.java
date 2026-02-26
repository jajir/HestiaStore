package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;

import org.hestiastore.index.directory.Directory;
import org.hestiastore.index.directory.MemDirectory;
import org.junit.jupiter.api.Test;

class TypeDescriptorLongTest {

    private final TypeDescriptorLong ti = new TypeDescriptorLong();
    private final TypeEncoder<Long> toBytes = ti.getTypeEncoder();
    private final TypeDecoder<Long> fromBytes = ti
            .getTypeDecoder();

    @Test
    void test_convertorto_bytes() {
        assertEqualsBytes(0L);
        assertEqualsBytes(21L);
        assertEqualsBytes(Long.MAX_VALUE);
        assertEqualsBytes(Long.MIN_VALUE);
        assertEqualsBytes(-1L);
    }

    private void assertEqualsBytes(Long number) {
        final byte[] bytes = TestEncoding.toByteArray(toBytes, number);
        final Long ret = fromBytes.decode(bytes);
        assertEquals(number, ret, String
                .format("Expected '%s' byt returned was '%s'", number, ret));
    }

    @Test
    void test_writer() {
        Directory dir = new MemDirectory();

        int ret = ti.getTypeWriter().write(dir.getFileWriter("test"),
                Long.MIN_VALUE);

        assertEquals(8, ret);
    }

    @Test
    void test_compare() {
        final Comparator<Long> cmp = ti.getComparator();
        assertEquals(0, cmp.compare(0l, 0L));
        assertTrue(cmp.compare(3l, 12L) < 0);
        assertTrue(cmp.compare(3l, 2L) > 0);
    }
}
