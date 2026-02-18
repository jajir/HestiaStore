package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;

import org.junit.jupiter.api.Test;

class TypeDescriptorIntegerTest {

    private final TypeDescriptorInteger ti = new TypeDescriptorInteger();
    private final ConvertorToBytes<Integer> toBytes = ti.getConvertorToBytes();
    private final ConvertorFromBytes<Integer> fromBytes = ti
            .getConvertorFromBytes();

    @Test
    void test_convertorto_bytes() {
        assertEqualsBytes(0);
        assertEqualsBytes(Integer.MAX_VALUE);
        assertEqualsBytes(Integer.MIN_VALUE);
        assertEqualsBytes(-1);
    }

    private void assertEqualsBytes(Integer number) {
        final byte[] bytes = toBytes.toBytes(number);
        final Integer ret = fromBytes.fromBytes(bytes);
        assertEquals(number, ret, String
                .format("Expected '%s' byt returned was '%s'", number, ret));
    }

    @Test
    void test_compare() {
        final Comparator<Integer> cmp = ti.getComparator();
        assertEquals(0, cmp.compare(0, 0));
        assertTrue(cmp.compare(3, 12) < 0);
        assertTrue(cmp.compare(3, 2) > 0);
    }

    @Test
    void test_isTombStone() {
        assertFalse(ti.isTombstone(-1));
        assertFalse(ti.isTombstone(Integer.MIN_VALUE));
        assertTrue(ti.isTombstone(Integer.MIN_VALUE + 1));
    }

}
