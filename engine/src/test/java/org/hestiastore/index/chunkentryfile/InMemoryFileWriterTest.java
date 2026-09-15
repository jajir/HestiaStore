package org.hestiastore.index.chunkentryfile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.hestiastore.index.bytes.ByteSequence;

class InMemoryFileWriterTest {

    @Test
    void repeatedCloseSequenceReturnsSamePayloadForEmptyAndNonEmptyPages() {
        for (final byte[] payload : new byte[][] { {}, { 1, 2 } }) {
            final InMemoryFileWriter writer = new InMemoryFileWriter(2);
            writer.write(payload);
            final ByteSequence result = writer.closeSequence();

            assertSame(result, writer.closeSequence());
            assertArrayEquals(payload, result.toByteArray());
            assertThrows(IllegalStateException.class,
                    () -> writer.write((byte) 3));
        }
    }

    @Test
    void writeByteArray_copiesSourceBuffer() {
        final byte[] reusableBuffer = new byte[] { 'a', 'a', 'a' };

        final InMemoryFileWriter writer = new InMemoryFileWriter();
        writer.write(reusableBuffer);
        reusableBuffer[0] = 'b';
        reusableBuffer[1] = 'b';
        reusableBuffer[2] = 'b';
        writer.write(reusableBuffer);

        assertArrayEquals(new byte[] { 'a', 'a', 'a', 'b', 'b', 'b' },
                writer.closeSequence().toByteArrayCopy());
    }

    @Test
    void writeByte_appendsSingleByte() {
        final InMemoryFileWriter writer = new InMemoryFileWriter();
        writer.write((byte) 'x');
        writer.write((byte) 'y');

        assertArrayEquals(new byte[] { 'x', 'y' },
                writer.closeSequence().toByteArrayCopy());
    }

    @Test
    void writeByteArrayRange_copiesOnlyRequestedSegment() {
        final byte[] source = new byte[] { '0', 'a', 'b', 'c', '9' };

        final InMemoryFileWriter writer = new InMemoryFileWriter();
        writer.write(source, 1, 3);
        source[1] = 'x';
        source[2] = 'y';
        source[3] = 'z';

        assertArrayEquals(new byte[] { 'a', 'b', 'c' },
                writer.closeSequence().toByteArrayCopy());
    }

    @Test
    void explicitCapacityRejectsGrowthBeyondBound() {
        final InMemoryFileWriter writer = new InMemoryFileWriter(2);

        writer.write(new byte[] { 1, 2 });

        assertThrows(IllegalStateException.class,
                () -> writer.write((byte) 3));
    }
}
