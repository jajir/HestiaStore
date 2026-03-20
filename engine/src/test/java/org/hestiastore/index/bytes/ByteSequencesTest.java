package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class ByteSequencesTest {

    @Test
    void test_wrap_and_view_of() {
        final byte[] data = new byte[] { 1, 2, 3, 4 };

        assertSame(ByteSequence.EMPTY, ByteSequences.wrap(new byte[0]));
        assertEquals(4, ByteSequences.wrap(data).length());
        assertArrayEquals(new byte[] { 2, 3 },
                ByteSequences.viewOf(data, 1, 3).toByteArrayCopy());
    }

    @Test
    void test_view_of_validates_range() {
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.viewOf(new byte[2], -1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.viewOf(new byte[2], 1, 0));
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.viewOf(new byte[2], 0, 3));
    }

    @Test
    void test_copy_of_array_and_sequence_are_defensive() {
        final byte[] sourceArray = new byte[] { 1, 2, 3 };
        final ByteSequence copiedFromArray = ByteSequences.copyOf(sourceArray);
        sourceArray[0] = 9;
        assertEquals(1, copiedFromArray.getByte(0));

        final MutableBytes mutable = MutableBytes.wrap(new byte[] { 4, 5, 6 });
        final ByteSequence copiedFromSequence = ByteSequences.copyOf(mutable);
        mutable.setByte(0, (byte) 9);
        assertEquals(4, copiedFromSequence.getByte(0));
    }

    @Test
    void test_padding_utilities() {
        final ByteSequence base = ByteSequences.wrap(new byte[] { 1, 2, 3 });

        final ByteSequence paddedLength = ByteSequences.padToLength(base, 5);
        assertEquals(5, paddedLength.length());
        assertArrayEquals(new byte[] { 1, 2, 3, 0, 0 },
                paddedLength.toByteArrayCopy());

        final ByteSequence paddedCell = ByteSequences.padToCell(base, 4);
        assertEquals(4, paddedCell.length());
        assertArrayEquals(new byte[] { 1, 2, 3, 0 }, paddedCell.toByteArrayCopy());

        assertSame(base, ByteSequences.padToLength(base, 2));
        assertSame(base, ByteSequences.padToCell(base, 3));
    }

    @Test
    void test_copy_with_offsets() {
        final ByteSequence source = ByteSequences.viewOf(new byte[] { 9, 8, 7, 6 },
                1, 4);
        final byte[] target = new byte[] { 0, 0, 0, 0, 0 };

        ByteSequences.copy(source, 1, target, 2, 2);

        assertArrayEquals(new byte[] { 0, 0, 7, 6, 0 }, target);
    }

    @Test
    void test_copy_validates_ranges() {
        final ByteSequence source = ByteSequences.wrap(new byte[] { 1, 2, 3 });
        final byte[] target = new byte[] { 0, 0 };

        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.copy(source, -1, target, 0, 1));
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.copy(source, 0, target, -1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.copy(source, 2, target, 0, 2));
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.copy(source, 0, target, 1, 2));
    }

    @Test
    void test_content_equals_and_hash_code() {
        final ByteSequence first = ByteSequences.wrap(new byte[] { 1, 2, 3 });
        final ByteSequence second = ByteSequences.viewOf(new byte[] { 9, 1, 2, 3 }, 1,
                4);
        final ByteSequence third = ByteSequences.wrap(new byte[] { 1, 2, 4 });

        assertTrue(ByteSequences.contentEquals(first, second));
        assertEquals(ByteSequences.contentHashCode(first),
                ByteSequences.contentHashCode(second));
        assertFalse(ByteSequences.contentEquals(first, third));
    }

    @Test
    void test_concat_empty_and_single() {
        assertSame(ByteSequence.EMPTY, ByteSequences.concat(List.of()));

        final ByteSequence only = ByteSequences.wrap(new byte[] { 1, 2 });
        assertSame(only, ByteSequences.concat(List.of(only, ByteSequence.EMPTY)));
    }

    @Test
    void test_concat_multiple_sequences_without_copy() {
        final ByteSequence first = ByteSequences.wrap(new byte[] { 1, 2 });
        final ByteSequence second = ByteSequences.wrap(new byte[] { 3, 4, 5 });
        final ByteSequence third = ByteSequences.wrap(new byte[] { 6 });

        final ByteSequence joined = ByteSequences
                .concat(List.of(first, ByteSequence.EMPTY, second, third));

        assertEquals(6, joined.length());
        assertArrayEquals(new byte[] { 1, 2, 3, 4, 5, 6 },
                joined.toByteArrayCopy());
        assertEquals(5, joined.getByte(4));
    }

    @Test
    void test_concat_validates_null_inputs() {
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.concat(null));
        final List<ByteSequence> withNull = new ArrayList<>();
        withNull.add(ByteSequence.EMPTY);
        withNull.add(null);
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.concat(withNull));
    }

    @Test
    void test_concatNonEmpty_validates_input() {
        final List<ByteSequence> emptySequences = List.of();
        final List<ByteSequence> emptyByteSequence = List.of(ByteSequence.EMPTY);

        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.concatNonEmpty(null));
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.concatNonEmpty(emptySequences));
        assertThrows(IllegalArgumentException.class,
                () -> ByteSequences.concatNonEmpty(emptyByteSequence));
    }

    @Test
    void test_concatNonEmpty_single_and_multiple() {
        final ByteSequence single = ByteSequences.wrap(new byte[] { 7 });
        assertSame(single, ByteSequences.concatNonEmpty(List.of(single)));

        final ByteSequence multi = ByteSequences.concatNonEmpty(
                List.of(ByteSequences.wrap(new byte[] { 1, 2 }),
                        ByteSequences.wrap(new byte[] { 3 }),
                        ByteSequences.wrap(new byte[] { 4, 5 })));
        assertArrayEquals(new byte[] { 1, 2, 3, 4, 5 },
                multi.toByteArrayCopy());
    }
}
