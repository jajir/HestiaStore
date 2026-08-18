package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class AbstractByteSequenceTest {

    @Test
    void test_copy_to_uses_generic_fallback_for_partial_range() {
        final GenericSequence sequence = new GenericSequence(
                new byte[] { 1, 2, 3, 4 });
        final byte[] target = new byte[] { 9, 9, 9, 9, 9 };

        sequence.copyTo(1, target, 2, 2);

        assertArrayEquals(new byte[] { 9, 9, 2, 3, 9 }, target);
        assertEquals(2, sequence.getByteCalls);
        assertEquals(0, sequence.toByteArrayCalls);
    }

    @Test
    void test_copy_to_accepts_zero_length_at_capacity() {
        final GenericSequence sequence = new GenericSequence(
                new byte[] { 1, 2, 3 });
        final byte[] target = new byte[] { 8, 9 };

        sequence.copyTo(sequence.length(), target, target.length, 0);

        assertArrayEquals(new byte[] { 8, 9 }, target);
        assertEquals(0, sequence.getByteCalls);
    }

    @Test
    void test_copy_to_rejects_null_target_even_when_length_is_zero() {
        final GenericSequence sequence = new GenericSequence(new byte[0]);

        assertThrows(IllegalArgumentException.class,
                () -> sequence.copyTo(0, null, 0, 0));
    }

    @Test
    void test_copy_to_rejects_negative_arguments() {
        final GenericSequence sequence = new GenericSequence(
                new byte[] { 1, 2, 3 });
        final byte[] target = new byte[3];

        assertThrows(IllegalArgumentException.class,
                () -> sequence.copyTo(-1, target, 0, 1));
        assertThrows(IllegalArgumentException.class,
                () -> sequence.copyTo(0, target, -1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> sequence.copyTo(0, target, 0, -1));
    }

    @Test
    void test_copy_to_rejects_ranges_outside_capacities() {
        final GenericSequence sequence = new GenericSequence(
                new byte[] { 1, 2, 3 });
        final byte[] target = new byte[2];

        assertThrows(IllegalArgumentException.class,
                () -> sequence.copyTo(4, target, 0, 0));
        assertThrows(IllegalArgumentException.class,
                () -> sequence.copyTo(0, target, 3, 0));
        assertThrows(IllegalArgumentException.class,
                () -> sequence.copyTo(2, target, 0, 2));
        assertThrows(IllegalArgumentException.class,
                () -> sequence.copyTo(0, target, 1, 2));
    }

    @Test
    void test_copy_to_rejects_overflowing_source_range() {
        final HookTrackingSequence sequence = new HookTrackingSequence(
                Integer.MAX_VALUE);
        final byte[] target = new byte[10];

        assertThrows(IllegalArgumentException.class, () -> sequence.copyTo(
                Integer.MAX_VALUE - 1, target, 0, 10));
        assertEquals(0, sequence.copyCalls);
    }

    @Test
    void test_copy_to_validates_before_modifying_target() {
        final HookTrackingSequence sequence = new HookTrackingSequence(3);
        final byte[] target = new byte[] { 7, 8, 9 };

        assertThrows(IllegalArgumentException.class,
                () -> sequence.copyTo(2, target, 0, 2));
        assertArrayEquals(new byte[] { 7, 8, 9 }, target);
        assertEquals(0, sequence.copyCalls);
    }

    private static final class GenericSequence extends AbstractByteSequence {

        private final byte[] data;
        private int getByteCalls;
        private int toByteArrayCalls;

        private GenericSequence(final byte[] data) {
            this.data = data;
        }

        @Override
        public int length() {
            return data.length;
        }

        @Override
        public byte getByte(final int index) {
            getByteCalls++;
            return data[index];
        }

        @Override
        public ByteSequence slice(final int fromInclusive,
                final int toExclusive) {
            return ByteSequences.viewOf(data, fromInclusive, toExclusive);
        }

        @Override
        public byte[] toByteArray() {
            toByteArrayCalls++;
            return data;
        }
    }

    private static final class HookTrackingSequence
            extends AbstractByteSequence {

        private final int length;
        private int copyCalls;

        private HookTrackingSequence(final int length) {
            this.length = length;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public byte getByte(final int index) {
            return 0;
        }

        @Override
        public ByteSequence slice(final int fromInclusive,
                final int toExclusive) {
            return ByteSequence.EMPTY;
        }

        @Override
        public byte[] toByteArray() {
            throw new UnsupportedOperationException("Not materialized.");
        }

        @Override
        protected void copyToWithValidatedInputs(final int sourceOffset,
                final byte[] target, final int targetOffset, final int length) {
            copyCalls++;
        }
    }
}
