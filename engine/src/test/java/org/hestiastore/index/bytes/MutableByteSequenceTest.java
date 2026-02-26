package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class MutableByteSequenceTest {

    @Test
    void test_default_set_bytes_uses_whole_source() {
        final TrackingMutableSequence sequence = new TrackingMutableSequence();
        final ByteSequence source = ByteSequences.wrap(new byte[] { 1, 2, 3 });

        sequence.setBytes(4, source);

        assertEquals(4, sequence.targetOffset);
        assertSame(source, sequence.source);
        assertEquals(0, sequence.sourceOffset);
        assertEquals(3, sequence.length);
    }

    @Test
    void test_default_set_bytes_validates_source() {
        final TrackingMutableSequence sequence = new TrackingMutableSequence();

        assertThrows(IllegalArgumentException.class, () -> sequence.setBytes(0, null));
    }

    private static final class TrackingMutableSequence implements MutableByteSequence {
        private int targetOffset;
        private ByteSequence source;
        private int sourceOffset;
        private int length;

        @Override
        public int length() {
            return 0;
        }

        @Override
        public byte getByte(final int index) {
            throw new IllegalArgumentException("No bytes.");
        }

        @Override
        public ByteSequence slice(final int fromInclusive, final int toExclusive) {
            return ByteSequence.EMPTY;
        }

        @Override
        public byte[] toByteArray() {
            return new byte[0];
        }

        @Override
        public void setByte(final int index, final byte value) {
            // not needed
        }

        @Override
        public void setBytes(final int targetOffset, final ByteSequence source,
                final int sourceOffset, final int length) {
            this.targetOffset = targetOffset;
            this.source = source;
            this.sourceOffset = sourceOffset;
            this.length = length;
        }
    }
}
