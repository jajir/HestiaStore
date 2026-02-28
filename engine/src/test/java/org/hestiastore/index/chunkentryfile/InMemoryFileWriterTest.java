package org.hestiastore.index.chunkentryfile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.hestiastore.index.bytes.BytesAppender;
import org.junit.jupiter.api.Test;

class InMemoryFileWriterTest {

    @Test
    void writeByteArray_copiesSourceBuffer() {
        final BytesAppender appender = new BytesAppender();
        final byte[] reusableBuffer = new byte[] { 'a', 'a', 'a' };

        try (final InMemoryFileWriter writer = new InMemoryFileWriter(
                appender)) {
            writer.write(reusableBuffer);
            reusableBuffer[0] = 'b';
            reusableBuffer[1] = 'b';
            reusableBuffer[2] = 'b';
            writer.write(reusableBuffer);
        }

        assertArrayEquals(new byte[] { 'a', 'a', 'a', 'b', 'b', 'b' },
                appender.getBytes().toByteArrayCopy());
    }

    @Test
    void writeByte_appendsSingleByte() {
        final BytesAppender appender = new BytesAppender();

        try (final InMemoryFileWriter writer = new InMemoryFileWriter(
                appender)) {
            writer.write((byte) 'x');
            writer.write((byte) 'y');
        }

        assertArrayEquals(new byte[] { 'x', 'y' },
                appender.getBytes().toByteArrayCopy());
    }

    @Test
    void writeByteArrayRange_copiesOnlyRequestedSegment() {
        final BytesAppender appender = new BytesAppender();
        final byte[] source = new byte[] { '0', 'a', 'b', 'c', '9' };

        try (final InMemoryFileWriter writer = new InMemoryFileWriter(
                appender)) {
            writer.write(source, 1, 3);
            source[1] = 'x';
            source[2] = 'y';
            source[3] = 'z';
        }

        assertArrayEquals(new byte[] { 'a', 'b', 'c' },
                appender.getBytes().toByteArrayCopy());
    }
}
