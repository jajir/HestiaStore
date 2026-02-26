package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class ByteSequenceCachingTest {

    @Test
    void test_to_byte_array_computes_once_and_caches() {
        final AtomicInteger counter = new AtomicInteger(0);
        final ByteSequenceCaching sequence = new CountingSequence(counter);

        final byte[] first = sequence.toByteArray();
        final byte[] second = sequence.toByteArray();

        assertSame(first, second);
        assertEquals(1, counter.get());
    }

    @Test
    void test_to_byte_array_rejects_null_compute_result() {
        final ByteSequenceCaching sequence = new NullSequence();

        assertThrows(IllegalArgumentException.class, sequence::toByteArray);
    }

    private static final class CountingSequence extends ByteSequenceCaching {
        private final AtomicInteger counter;

        private CountingSequence(final AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public int length() {
            return 1;
        }

        @Override
        public byte getByte(final int index) {
            if (index != 0) {
                throw new IllegalArgumentException("Only index 0 is supported.");
            }
            return 7;
        }

        @Override
        public ByteSequence slice(final int fromInclusive, final int toExclusive) {
            if (fromInclusive == 0 && toExclusive == 1) {
                return this;
            }
            if (fromInclusive == 0 && toExclusive == 0) {
                return ByteSequence.EMPTY;
            }
            throw new IllegalArgumentException("Invalid range.");
        }

        @Override
        protected byte[] computeByteArray() {
            counter.incrementAndGet();
            return new byte[] { 7 };
        }
    }

    private static final class NullSequence extends ByteSequenceCaching {
        @Override
        public int length() {
            return 0;
        }

        @Override
        public byte getByte(final int index) {
            throw new IllegalArgumentException("Empty sequence.");
        }

        @Override
        public ByteSequence slice(final int fromInclusive, final int toExclusive) {
            return ByteSequence.EMPTY;
        }

        @Override
        protected byte[] computeByteArray() {
            return null;
        }
    }
}
