package org.hestiastore.index.chunkstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hestiastore.index.Bytes;
import org.junit.jupiter.api.Test;

public class BytesAppenderTest {

    @Test
    void test_append() {
        BytesAppender appender = new BytesAppender();
        appender.append(Bytes.of(new byte[] { 1, 2, 3 }));
        appender.append(Bytes.of(new byte[] { 4, 5, 6 }));
        Bytes result = appender.getBytes();
        assertEquals(6, result.length());
        assertEquals(1, result.getByte(0));
        assertEquals(2, result.getByte(1));
        assertEquals(3, result.getByte(2));
        assertEquals(4, result.getByte(3));
        assertEquals(5, result.getByte(4));
        assertEquals(6, result.getByte(5));
    }

    @Test
    void test_append_empty_array() {
        BytesAppender appender = new BytesAppender();
        appender.append(Bytes.EMPTY);
        appender.append(Bytes.of(new byte[] { 1, 2, 3 }));
        appender.append(Bytes.EMPTY);
        appender.append(Bytes.of(new byte[] { 4, 5, 6 }));
        appender.append(Bytes.EMPTY);
        appender.append(Bytes.EMPTY);
        appender.append(Bytes.EMPTY);
        Bytes result = appender.getBytes();
        assertEquals(6, result.length());
        assertEquals(1, result.getByte(0));
        assertEquals(2, result.getByte(1));
        assertEquals(3, result.getByte(2));
        assertEquals(4, result.getByte(3));
        assertEquals(5, result.getByte(4));
        assertEquals(6, result.getByte(5));
    }

    @Test
    void test_append_null() {
        BytesAppender appender = new BytesAppender();
        final Exception e = assertThrows(IllegalArgumentException.class,
                () -> appender.append(null));

        assertEquals("Property 'bytes' must not be null.", e.getMessage());
    }

}
