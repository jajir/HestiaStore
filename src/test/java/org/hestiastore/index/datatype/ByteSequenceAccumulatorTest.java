package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.hestiastore.index.bytes.ByteSequence;
import org.hestiastore.index.bytes.ByteSequences;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.bytes.MutableBytes;
import org.junit.jupiter.api.Test;

class ByteSequenceAccumulatorTest {

    @Test
    void writeByte_appendsSingleByte() {
        try (ByteSequenceAccumulator writer = new ByteSequenceAccumulator()) {
            writer.write((byte) 0x2A);
            writer.write((byte) 0x2B);

            assertArrayEquals(new byte[] { 0x2A, 0x2B }, writer.toByteArray());
        }
    }

    @Test
    void writeSequence_appendsAllBytes() {
        final ByteSequence buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });
        try (ByteSequenceAccumulator writer = new ByteSequenceAccumulator()) {
            writer.write(buffer);
            writer.write(buffer);

            assertArrayEquals(new byte[] { 1, 2, 3, 1, 2, 3 },
                    writer.toByteArray());
        }
    }

    @Test
    void writeSequence_nullThrows() {
        try (ByteSequenceAccumulator writer = new ByteSequenceAccumulator()) {
            assertThrows(IllegalArgumentException.class,
                    () -> writer.write((ByteSequence) null));
        }
    }

    @Test
    void toByteArray_returnsCopy() {
        try (ByteSequenceAccumulator writer = new ByteSequenceAccumulator()) {
            writer.write((byte) 1);

            final byte[] first = writer.toByteArray();
            first[0] = 9;

            assertArrayEquals(new byte[] { 1 }, writer.toByteArray());
        }
    }

    @Test
    void toBytes_returnsImmutableSnapshot() {
        try (ByteSequenceAccumulator writer = new ByteSequenceAccumulator()) {
            writer.write((byte) 7);
            writer.write((byte) 8);

            final ByteSequence snapshot = writer.toBytes();

            assertEquals(ByteSequences.wrap(new byte[] { 7, 8 }), snapshot);

            writer.write((byte) 9);

            assertEquals(ByteSequences.wrap(new byte[] { 7, 8 }), snapshot);
            assertArrayEquals(new byte[] { 7, 8, 9 }, writer.toByteArray());
        }
    }

    @Test
    void close_wrapsIOExceptionAsIndexException() {
        final ByteSequenceAccumulator writer = new ByteSequenceAccumulator() {
            @Override
            protected void doClose() {
                throw new IndexException("boom", new RuntimeException());
            }
        };

        final IndexException ex = assertThrows(IndexException.class,
                writer::close);
        assertEquals("boom", ex.getMessage());
    }

    @Test
    void close_marksWriterClosedAndPreventsReclose() {
        final ByteSequenceAccumulator writer = new ByteSequenceAccumulator();

        assertFalse(writer.wasClosed());

        writer.close();

        assertTrue(writer.wasClosed());
        assertThrows(IllegalStateException.class, writer::close);
    }

    @Test
    void writeEmptySequence_doesNotChangeContent() {
        final ByteSequence empty = MutableBytes.wrap(new byte[0]);
        try (ByteSequenceAccumulator writer = new ByteSequenceAccumulator()) {
            writer.write(empty);

            assertArrayEquals(new byte[0], writer.toByteArray());
        }
    }

    @Test
    void writeSequence_reusesBackingBuffer() {
        final MutableBytes buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });
        try (ByteSequenceAccumulator writer = new ByteSequenceAccumulator()) {
            writer.write(buffer);

            buffer.setByte(1, (byte) 9);

            assertArrayEquals(new byte[] { 1, 9, 3 }, writer.toByteArray());
        }
    }

}
