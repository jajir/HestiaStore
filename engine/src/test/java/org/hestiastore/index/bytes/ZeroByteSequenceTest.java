package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ZeroByteSequenceTest {

    @Test
    void test_constructor_validates_length() {
        assertThrows(IllegalArgumentException.class, () -> new ZeroByteSequence(-1));
    }

    @Test
    void test_zero_sequence_basics() {
        final ZeroByteSequence sequence = new ZeroByteSequence(3);

        assertEquals(3, sequence.length());
        assertEquals(0, sequence.getByte(0));
        assertEquals(0, sequence.getByte(2));
        assertArrayEquals(new byte[] { 0, 0, 0 }, sequence.toByteArray());
    }

    @Test
    void test_slice_behaviour() {
        final ZeroByteSequence sequence = new ZeroByteSequence(4);

        assertSame(sequence, sequence.slice(0, 4));
        assertSame(ByteSequence.EMPTY, sequence.slice(2, 2));
        assertEquals(2, sequence.slice(1, 3).length());
    }

    @Test
    void test_slice_and_get_byte_validate_bounds() {
        final ZeroByteSequence sequence = new ZeroByteSequence(1);

        assertThrows(IllegalArgumentException.class, () -> sequence.getByte(1));
        assertThrows(IllegalArgumentException.class, () -> sequence.slice(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> sequence.slice(0, 2));
    }

    @Test
    void test_equals_and_hash_code_are_content_based() {
        final ZeroByteSequence zeros = new ZeroByteSequence(2);
        final ByteSequence other = ByteSequences.wrap(new byte[] { 0, 0 });

        assertEquals(zeros, other);
        assertEquals(zeros.hashCode(), other.hashCode());
    }
}
