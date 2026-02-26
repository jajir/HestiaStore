package org.hestiastore.index.chunkentryfile;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.hestiastore.index.chunkstore.BytesAppender;
import org.junit.jupiter.api.Test;

class InMemoryFileWriterTest {

    @Test
    void writeByteArray_copiesSourceBuffer() {
        final BytesAppender appender = new BytesAppender();
        final InMemoryFileWriter writer = new InMemoryFileWriter(appender);
        final byte[] reusableBuffer = new byte[] { 'a', 'a', 'a' };

        writer.write(reusableBuffer);
        reusableBuffer[0] = 'b';
        reusableBuffer[1] = 'b';
        reusableBuffer[2] = 'b';
        writer.write(reusableBuffer);

        assertArrayEquals(new byte[] { 'a', 'a', 'a', 'b', 'b', 'b' },
                appender.getBytes().getData());
    }
}
