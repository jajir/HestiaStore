package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ByteSequenceViewTest {

    @Test
    void test_of_empty_returns_singleton() {
        final ByteSequenceView view1 = ByteSequenceView.of(new byte[0]);
        final ByteSequenceView view2 = ByteSequenceView.of(new byte[0]);

        assertSame(view1, view2);
        assertSame(ByteSequence.EMPTY, view1.slice(0, 0));
    }

    @Test
    void test_to_byte_array_returns_backing_array() {
        final byte[] data = new byte[] { 1, 2, 3 };
        final ByteSequenceView view = ByteSequenceView.of(data);

        assertSame(data, view.toByteArray());
    }

    @Test
    void test_slice_full_range_returns_this() {
        final ByteSequenceView view = ByteSequenceView.of(new byte[] { 1, 2, 3 });

        assertSame(view, view.slice(0, 3));
    }

    @Test
    void test_slice_returns_subsequence() {
        final ByteSequenceView view = ByteSequenceView.of(new byte[] { 9, 8, 7, 6 });
        final ByteSequence slice = view.slice(1, 3);

        assertEquals(2, slice.length());
        assertEquals(8, slice.getByte(0));
        assertEquals(7, slice.getByte(1));
    }

    @Test
    void test_get_byte_out_of_bounds_fails() {
        final ByteSequenceView view = ByteSequenceView.of(new byte[] { 1 });

        assertThrows(IllegalArgumentException.class, () -> view.getByte(-1));
        assertThrows(IllegalArgumentException.class, () -> view.getByte(1));
    }

    @Test
    void test_equals_hash_code_with_same_content() {
        final ByteSequenceView first = ByteSequenceView.of(new byte[] { 1, 2, 3 });
        final ByteSequence second = ByteSequences.viewOf(new byte[] { 9, 1, 2, 3 },
                1, 4);

        assertTrue(first.equals(second));
        assertEquals(first.hashCode(), second.hashCode());
    }
}
