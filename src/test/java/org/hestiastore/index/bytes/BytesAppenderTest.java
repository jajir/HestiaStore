package org.hestiastore.index.bytes;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class BytesAppenderTest {

    @Test
    void append_emptySequenceIsIgnored() {
        BytesAppender appender = new BytesAppender();

        appender.append(Bytes.EMPTY);

        assertEquals(0, appender.getBytes().length());
    }

    @Test
    void append_singleSequenceReturnsSameContent() {
        BytesAppender appender = new BytesAppender();
        ByteSequence data = Bytes.of(new byte[] { 1, 2, 3 });

        appender.append(data);

        assertArrayEquals(new byte[] { 1, 2, 3 },
                appender.getBytes().toByteArray());
    }

    @Test
    void append_multipleSequencesConcatenates() {
        BytesAppender appender = new BytesAppender();

        appender.append(Bytes.of(new byte[] { 1, 2 }));
        appender.append(Bytes.of(new byte[] { 3 }));
        appender.append(Bytes.of(new byte[] { 4, 5, 6 }));

        assertArrayEquals(new byte[] { 1, 2, 3, 4, 5, 6 },
                appender.getBytes().toByteArray());
    }

    @Test
    void append_supportsByteSequenceViews() {
        BytesAppender appender = new BytesAppender();
        byte[] backing = { 9, 8, 7, 6 };
        ByteSequence view = ByteSequenceView.of(backing, 1, 3);

        appender.append(view);
        backing[1] = 5; // ensure the view is read on access

        assertArrayEquals(new byte[] { 5, 7 },
                appender.getBytes().toByteArray());
    }
}
