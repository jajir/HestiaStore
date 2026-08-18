package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

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
    void test_copy_from_first_part() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2, 3 }),
                ByteSequences.wrap(new byte[] { 4, 5, 6 }));
        final byte[] target = new byte[] { 9, 9, 9, 9, 9 };

        concatenated.copyTo(1, target, 1, 2);

        assertArrayEquals(new byte[] { 9, 2, 3, 9, 9 }, target);
    }

    @Test
    void test_copy_from_second_part() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2, 3 }),
                ByteSequences.wrap(new byte[] { 4, 5, 6 }));
        final byte[] target = new byte[] { 9, 9, 9, 9 };

        concatenated.copyTo(4, target, 1, 2);

        assertArrayEquals(new byte[] { 9, 5, 6, 9 }, target);
    }

    @Test
    void test_copy_across_boundary() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2, 3 }),
                ByteSequences.wrap(new byte[] { 4, 5, 6 }));
        final byte[] target = new byte[] { 9, 9, 9, 9, 9 };

        concatenated.copyTo(2, target, 1, 3);

        assertArrayEquals(new byte[] { 9, 3, 4, 5, 9 }, target);
    }

    @Test
    void test_copy_from_exact_boundary() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2, 3 }),
                ByteSequences.wrap(new byte[] { 4, 5, 6 }));
        final byte[] target = new byte[] { 9, 9, 9, 9 };

        concatenated.copyTo(3, target, 1, 2);

        assertArrayEquals(new byte[] { 9, 4, 5, 9 }, target);
    }

    @Test
    void test_copy_one_byte_from_each_part() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2, 3 }),
                ByteSequences.wrap(new byte[] { 4, 5, 6 }));
        final byte[] target = new byte[] { 9, 9, 9, 9 };

        concatenated.copyTo(2, target, 1, 2);

        assertArrayEquals(new byte[] { 9, 3, 4, 9 }, target);
    }

    @Test
    void test_copy_full_concatenation() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2 }),
                new ZeroByteSequence(2));
        final byte[] target = new byte[] { 9, 9, 9, 9, 9, 9 };

        concatenated.copyTo(0, target, 1, concatenated.length());

        assertArrayEquals(new byte[] { 9, 1, 2, 0, 0, 9 }, target);
    }

    @Test
    void test_copy_zero_length_at_boundary_and_end() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2 }),
                ByteSequences.wrap(new byte[] { 3, 4 }));
        final byte[] target = new byte[] { 8, 9 };

        concatenated.copyTo(2, target, 1, 0);
        concatenated.copyTo(4, target, 2, 0);

        assertArrayEquals(new byte[] { 8, 9 }, target);
    }

    @Test
    void test_copy_rejects_invalid_range_before_modifying_target() {
        final ByteSequence concatenated = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2 }),
                ByteSequences.wrap(new byte[] { 3, 4 }));
        final byte[] target = new byte[] { 8, 9 };

        assertThrows(IllegalArgumentException.class,
                () -> concatenated.copyTo(3, target, 0, 2));
        assertArrayEquals(new byte[] { 8, 9 }, target);
    }

    @Test
    void test_copy_from_nested_concatenation() {
        final ByteSequence concatenated = ByteSequences.concatNonEmpty(List.of(
                ByteSequences.wrap(new byte[] { 1, 2 }),
                ByteSequences.wrap(new byte[] { 3, 4 }),
                ByteSequences.wrap(new byte[] { 5, 6 }),
                ByteSequences.wrap(new byte[] { 7, 8 })));
        final byte[] target = new byte[] { 9, 9, 9, 9, 9, 9, 9, 9 };

        concatenated.copyTo(1, target, 1, 6);

        assertArrayEquals(new byte[] { 9, 2, 3, 4, 5, 6, 7, 9 }, target);
    }

    @Test
    void test_equals_hash_and_validations() {
        final ByteSequence one = ConcatenatedByteSequence.of(
                ByteSequences.wrap(new byte[] { 1, 2 }),
                ByteSequences.wrap(new byte[] { 3 }));
        final ByteSequence other = ByteSequences.wrap(new byte[] { 1, 2, 3 });

        assertEquals(one, other);
        assertEquals(one.hashCode(), other.hashCode());
        assertThrows(IllegalArgumentException.class, () -> one.getByte(3));
        assertThrows(IllegalArgumentException.class, () -> one.slice(-1, 1));
    }
}
