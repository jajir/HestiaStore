package org.hestiastore.index.directory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class MemFileReaderTest {

    @Test
    void read_returnsUnsignedByteValues() {
        final MemFileReader reader = new MemFileReader(
                new byte[] { 0, (byte) 0xFF });

        assertEquals(0, reader.read());
        assertEquals(255, reader.read());
        assertEquals(-1, reader.read());
    }
}
