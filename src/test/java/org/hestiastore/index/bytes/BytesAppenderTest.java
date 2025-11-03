package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class BytesAppenderTest {

    @Test
    void append_emptySequenceIsIgnored() {
        BytesAppender appender = new BytesAppender();

        appender.append(ByteSequence.EMPTY);

        assertEquals(0, appender.getBytes().length());
    }

    @Test
    void append_singleSequenceReturnsSameContent() {
        BytesAppender appender = new BytesAppender();
        ByteSequence data = ByteSequences.wrap(new byte[] { 1, 2, 3 });

        appender.append(data);

        assertArrayEquals(new byte[] { 1, 2, 3 },
                appender.getBytes().toByteArray());
    }

    @Test
    void append_multipleSequencesConcatenates() {
        BytesAppender appender = new BytesAppender();

        appender.append(ByteSequences.wrap(new byte[] { 1, 2 }));
        appender.append(ByteSequences.wrap(new byte[] { 3 }));
        appender.append(ByteSequences.wrap(new byte[] { 4, 5, 6 }));

        assertArrayEquals(new byte[] { 1, 2, 3, 4, 5, 6 },
                appender.getBytes().toByteArray());
    }

    @Test
    void append_supportsSlicedSequences() {
        BytesAppender appender = new BytesAppender();
        byte[] backing = { 9, 8, 7, 6 };
        ByteSequence view = ByteSequences.viewOf(backing, 1, 3);

        appender.append(view);
        backing[1] = 5; // ensure the view is read on access

        assertArrayEquals(new byte[] { 5, 7 },
                appender.getBytes().toByteArray());
    }
}
