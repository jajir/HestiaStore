package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ConcatenatedByteSequenceTest {

    @Test
    void test_of_optimizes_empty_inputs() {
        final ByteSequence one = ByteSequences.wrap(new byte[] { 1 });

        assertSame(one, ConcatenatedByteSequence.of(ByteSequence.EMPTY, one));
        assertSame(one, ConcatenatedByteSequence.of(one, ByteSequence.EMPTY));
    }

    @Test
    void test_concatenation_and_get_byte() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2 }),
                ByteSequences.wrap(new byte[] { 3, 4 }));

        assertEquals(4, concatenated.length());
        assertEquals(1, concatenated.getByte(0));
        assertEquals(4, concatenated.getByte(3));
        assertArrayEquals(new byte[] { 1, 2, 3, 4 }, concatenated.toByteArrayCopy());
    }

    @Test
    void test_slice_across_both_sequences() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2, 3 }),
                ByteSequences.wrap(new byte[] { 4, 5, 6 }));

        final ByteSequence firstPart = concatenated.slice(0, 2);
        final ByteSequence secondPart = concatenated.slice(4, 6);
        final ByteSequence crossPart = concatenated.slice(2, 5);

        assertArrayEquals(new byte[] { 1, 2 }, firstPart.toByteArrayCopy());
        assertArrayEquals(new byte[] { 5, 6 }, secondPart.toByteArrayCopy());
        assertArrayEquals(new byte[] { 3, 4, 5 }, crossPart.toByteArrayCopy());
    }

    @Test
    void test_equals_hash_and_validations() {
        final ByteSequence one = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2 }),
                ByteSequences.wrap(new byte[] { 3 }));
        final ByteSequence other = ByteSequences.wrap(new byte[] { 1, 2, 3 });

        assertTrue(one.equals(other));
        assertEquals(one.hashCode(), other.hashCode());
        assertThrows(IllegalArgumentException.class, () -> one.getByte(3));
        assertThrows(IllegalArgumentException.class, () -> one.slice(-1, 1));
    }
}
