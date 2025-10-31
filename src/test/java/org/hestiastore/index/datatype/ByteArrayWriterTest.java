package org.hestiastore.index.datatype;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.hestiastore.index.ByteSequence;
import org.hestiastore.index.Bytes;
import org.hestiastore.index.IndexException;
import org.hestiastore.index.MutableBytes;
import org.junit.jupiter.api.Test;

class ByteArrayWriterTest {

    @Test
    void writeByte_appendsSingleByte() {
        try (ByteArrayWriter writer = new ByteArrayWriter()) {
            writer.write((byte) 0x2A);
            writer.write((byte) 0x2B);

            assertArrayEquals(new byte[] { 0x2A, 0x2B }, writer.toByteArray());
        }
    }

    @Test
    void writeSequence_appendsAllBytes() {
        final ByteSequence buffer = MutableBytes.wrap(new byte[] { 1, 2, 3 });
        try (ByteArrayWriter writer = new ByteArrayWriter()) {
            writer.write(buffer);
            writer.write(buffer);

            assertArrayEquals(new byte[] { 1, 2, 3, 1, 2, 3 },
                    writer.toByteArray());
        }
    }

    @Test
    void writeSequence_nullThrows() {
        try (ByteArrayWriter writer = new ByteArrayWriter()) {
            assertThrows(IllegalArgumentException.class,
                    () -> writer.write((ByteSequence) null));
        }
    }

    @Test
    void toByteArray_returnsCopy() {
        try (ByteArrayWriter writer = new ByteArrayWriter()) {
            writer.write((byte) 1);

            final byte[] first = writer.toByteArray();
            first[0] = 9;

            assertArrayEquals(new byte[] { 1 }, writer.toByteArray());
        }
    }

    @Test
    void toBytes_returnsImmutableSnapshot() {
        try (ByteArrayWriter writer = new ByteArrayWriter()) {
            writer.write((byte) 7);
            writer.write((byte) 8);

            final Bytes snapshot = writer.toBytes();

            assertEquals(Bytes.of(new byte[] { 7, 8 }), snapshot);

            writer.write((byte) 9);

            assertEquals(Bytes.of(new byte[] { 7, 8 }), snapshot);
            assertArrayEquals(new byte[] { 7, 8, 9 }, writer.toByteArray());
        }
    }

    @Test
    void close_wrapsIOExceptionAsIndexException() {
        final ByteArrayWriter writer = new ByteArrayWriter() {
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
        final ByteArrayWriter writer = new ByteArrayWriter();

        assertFalse(writer.wasClosed());

        writer.close();

        assertTrue(writer.wasClosed());
        assertThrows(IllegalStateException.class, writer::close);
    }

    @Test
    void writeEmptySequence_doesNotChangeContent() {
        final ByteSequence empty = MutableBytes.wrap(new byte[0]);
        try (ByteArrayWriter writer = new ByteArrayWriter()) {
            writer.write(empty);

            assertArrayEquals(new byte[0], writer.toByteArray());
        }
    }

}
