package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ByteSequenceSliceTest {

    @Test
    void test_constructor_validates_arguments() {
        assertThrows(IllegalArgumentException.class,
                () -> new ByteSequenceSlice(null, 0, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new ByteSequenceSlice(new byte[1], -1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new ByteSequenceSlice(new byte[1], 0, -1));
        assertThrows(IllegalArgumentException.class,
                () -> new ByteSequenceSlice(new byte[1], 1, 1));
    }

    @Test
    void test_slice_nested_full_and_empty() {
        final ByteSequenceSlice sequence = new ByteSequenceSlice(
                new byte[] { 1, 2, 3, 4 }, 1, 2);

        assertSame(sequence, sequence.slice(0, 2));
        assertSame(ByteSequence.EMPTY, sequence.slice(1, 1));
        assertThrows(IllegalArgumentException.class, () -> sequence.slice(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> sequence.slice(0, 3));
    }

    @Test
    void test_to_byte_array_is_cached() {
        final ByteSequenceSlice sequence = new ByteSequenceSlice(
                new byte[] { 1, 2, 3, 4 }, 1, 2);

        final byte[] first = sequence.toByteArray();
        final byte[] second = sequence.toByteArray();

        assertSame(first, second);
        assertArrayEquals(new byte[] { 2, 3 }, first);
    }

    @Test
    void test_copy_to_uses_range_inside_backing_array() {
        final ByteSequenceSlice sequence = new ByteSequenceSlice(
                new byte[] { 9, 8, 7, 6 }, 1, 2);
        final byte[] target = new byte[] { 0, 0, 0, 0 };

        ByteSequences.copy(sequence, 0, target, 1, 2);

        assertArrayEquals(new byte[] { 0, 8, 7, 0 }, target);
    }

    @Test
    void test_equals_hash_code_and_get_byte() {
        final ByteSequenceSlice first = new ByteSequenceSlice(
                new byte[] { 5, 6, 7, 8 }, 1, 2);
        final ByteSequence second = ByteSequences.wrap(new byte[] { 6, 7 });

        assertEquals(6, first.getByte(0));
        assertEquals(7, first.getByte(1));
        assertTrue(first.equals(second));
        assertEquals(first.hashCode(), second.hashCode());
        assertThrows(IllegalArgumentException.class, () -> first.getByte(2));
    }
}
