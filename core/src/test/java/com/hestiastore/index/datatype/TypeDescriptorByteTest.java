package com.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class TypeDescriptorByteTest {

    private final TypeDescriptorByte ti = new TypeDescriptorByte();

    @Test
    public void test_isTombstone() throws Exception {
        assertFalse(ti.isTombstone(Byte.valueOf((byte) 127)));
        assertFalse(ti.isTombstone(Byte.valueOf((byte) 1)));
        assertFalse(ti.isTombstone(Byte.valueOf((byte) 0)));
        assertFalse(ti.isTombstone(Byte.valueOf((byte) -1)));
        assertFalse(ti.isTombstone(Byte.valueOf((byte) -127)));
        assertTrue(ti.isTombstone(Byte.valueOf((byte) -128)));
    }

}
