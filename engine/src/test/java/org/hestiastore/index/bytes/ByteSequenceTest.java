package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ByteSequenceTest {

    @Test
    void test_empty_sequence_contract() {
        assertEquals(0, ByteSequence.EMPTY.length());
        assertTrue(ByteSequence.EMPTY.isEmpty());
        assertSame(ByteSequence.EMPTY, ByteSequence.EMPTY.slice(0, 0));
        assertArrayEquals(new byte[0], ByteSequence.EMPTY.toByteArray());
        assertArrayEquals(new byte[0], ByteSequence.EMPTY.toByteArrayCopy());
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequence.EMPTY.getByte(0));
    }

    @Test
    void test_to_byte_array_copy_is_defensive() {
        final byte[] source = new byte[] { 1, 2, 3 };
        final ByteSequence sequence = ByteSequences.wrap(source);

        final byte[] copy = sequence.toByteArrayCopy();
        copy[0] = 9;

        assertEquals(1, sequence.getByte(0));
    }
}
