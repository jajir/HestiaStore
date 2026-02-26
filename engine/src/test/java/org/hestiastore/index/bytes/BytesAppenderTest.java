package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class BytesAppenderTest {

    @Test
    void test_append_validates_input() {
        final BytesAppender appender = new BytesAppender();

        assertThrows(IllegalArgumentException.class, () -> appender.append(null));
    }

    @Test
    void test_get_bytes_ignores_empty_sequences() {
        final BytesAppender appender = new BytesAppender();
        appender.append(ByteSequence.EMPTY);
        appender.append(ByteSequences.wrap(new byte[] { 1, 2 }));
        appender.append(ByteSequence.EMPTY);

        final ByteSequence result = appender.getBytes();
        assertArrayEquals(new byte[] { 1, 2 }, result.toByteArrayCopy());
    }

    @Test
    void test_get_bytes_empty_when_nothing_was_appended() {
        final BytesAppender appender = new BytesAppender();

        assertSame(ByteSequence.EMPTY, appender.getBytes());
    }
}
