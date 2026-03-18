package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class MutableBytesTest {

    @Test
    void test_allocate_and_wrap() {
        final MutableBytes allocated = MutableBytes.allocate(3);
        assertEquals(3, allocated.length());
        assertArrayEquals(new byte[] { 0, 0, 0 }, allocated.array());

        final byte[] data = new byte[] { 1, 2 };
        final MutableBytes wrapped = MutableBytes.wrap(data);
        assertSame(data, wrapped.array());
        assertThrows(IllegalArgumentException.class, () -> MutableBytes.allocate(-1));
    }

    @Test
    void test_get_set_and_slice() {
        final MutableBytes bytes = MutableBytes.wrap(new byte[] { 1, 2, 3, 4 });

        bytes.setByte(1, (byte) 9);
        assertEquals(9, bytes.getByte(1));

        final ByteSequence full = bytes.slice(0, 4);
        final ByteSequence part = bytes.slice(1, 3);
        assertArrayEquals(new byte[] { 9, 3 }, part.toByteArrayCopy());
        assertSame(ByteSequence.EMPTY, bytes.slice(2, 2));
        assertEquals(4, full.length());
    }

    @Test
    void test_set_bytes() {
        final MutableBytes target = MutableBytes.wrap(new byte[] { 0, 0, 0, 0 });
        final ByteSequence source = ByteSequences.wrap(new byte[] { 8, 7, 6 });

        target.setBytes(1, source, 1, 2);
        assertArrayEquals(new byte[] { 0, 7, 6, 0 }, target.array());
    }

    @Test
    void test_equals_hash_code_and_to_byte_array() {
        final MutableBytes first = MutableBytes.wrap(new byte[] { 1, 2, 3 });
        final ByteSequence second = ByteSequences.wrap(new byte[] { 1, 2, 3 });

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertSame(first.array(), first.toByteArray());
    }
}
